# -*- coding: utf-8 -*-
"""
Feishu Approval Component

A pipeline node that sends an interactive Feishu card to reviewers with
Approve / Reject buttons, waits for all reviewers to decide, then outputs
the final result ("approved" or "rejected").

Execution flow:
  execute() — create FeishuApprovalRecord, send cards, enter schedule wait
  schedule() — wait for bamboo callback_data; finish when callback carries final result
"""
import json
import logging
import secrets

import requests

from pipeline.component_framework.component import Component
from pipeline.core.flow.activity import Service
from pipeline.core.flow.io import StringItemSchema
from components.schemas import ExtendedArraySchema

logger = logging.getLogger('django')

FEISHU_BASE_URL = 'https://open.feishu.cn/open-apis'


def _get_access_token() -> str:
    """Return a fresh tenant_access_token or raise ValueError."""
    from config.models import PlatformConfig
    cfg = PlatformConfig.get_feishu_config()
    app_id = cfg.get('app_id', '')
    app_secret = cfg.get('app_secret', '')
    if not app_id or not app_secret:
        raise ValueError('Feishu app_id / app_secret not configured in platform settings')

    resp = requests.post(
        f'{FEISHU_BASE_URL}/auth/v3/tenant_access_token/internal',
        json={'app_id': app_id, 'app_secret': app_secret},
        timeout=10,
    )
    data = resp.json()
    if data.get('code') != 0:
        raise ValueError(f"Failed to get Feishu access token: {data.get('msg')}")
    return data['tenant_access_token']


def _build_approval_card(
    content: str,
    token: str,
    open_id: str,
    callback_node_id: str,
    callback_node_version: str,
) -> dict:
    """
    Build a Feishu Card JSON v2 with Approve / Reject buttons.

    Each reviewer gets a personalised card with their own open_id embedded
    in the button value, so the callback knows who clicked.
    """
    return {
        'schema': '2.0',
        'config': {'update_multi': True},
        'header': {
            'title': {'tag': 'plain_text', 'content': '📋 审核请求'},
            'template': 'orange',
        },
        'body': {
            'elements': [
                {
                    'tag': 'markdown',
                    'content': f'**审核内容**\n{content}',
                },
                {'tag': 'hr'},
                {
                    'tag': 'button',
                    'text': {'tag': 'plain_text', 'content': '✅ 通过'},
                    'type': 'primary',
                    'behaviors': [
                        {
                            'type': 'callback',
                            'value': {
                                'action_type': 'feishu_approval',
                                'token': token,
                                'decision': '1',
                                'open_id': open_id,
                                'node_id': callback_node_id,
                                'node_version': callback_node_version,
                            },
                        }
                    ],
                },
                {
                    'tag': 'button',
                    'text': {'tag': 'plain_text', 'content': '❌ 不通过'},
                    'type': 'danger',
                    'behaviors': [
                        {
                            'type': 'callback',
                            'value': {
                                'action_type': 'feishu_approval',
                                'token': token,
                                'decision': '0',
                                'open_id': open_id,
                                'node_id': callback_node_id,
                                'node_version': callback_node_version,
                            },
                        }
                    ],
                },
            ]
        },
    }


def _send_card(access_token: str, receiver_open_id: str, card: dict) -> str:
    """
    Send an interactive card message to a single Feishu user.
    Returns the message_id on success, empty string on failure.
    """
    resp = requests.post(
        f'{FEISHU_BASE_URL}/im/v1/messages',
        params={'receive_id_type': 'open_id'},
        headers={
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json; charset=utf-8',
        },
        json={
            'receive_id': receiver_open_id,
            'msg_type': 'interactive',
            'content': json.dumps(card),
        },
        timeout=10,
    )
    data = resp.json()
    if data.get('code') != 0:
        logger.warning(
            f'Failed to send approval card to open_id={receiver_open_id}: {data.get("msg")}'
        )
        return ''
    return data.get('data', {}).get('message_id', '')


class FeishuApprovalService(Service):
    """
    Pipeline Service — Feishu Approval Node.

    execute(): send cards to all reviewers, save state to DB, enter schedule.
    schedule(): wait callback_data from bamboo_engine.api.callback and output final result.
    """

    __need_schedule__ = True
    interval = None

    # ------------------------------------------------------------------ #
    # execute                                                              #
    # ------------------------------------------------------------------ #
    def execute(self, data, parent_data):
        content = data.get_one_of_inputs('content', '')
        reviewer_ids = data.get_one_of_inputs('reviewer_ids', [])

        if not content:
            data.set_outputs('result', '')
            data.set_outputs('_error', '审核内容不能为空')
            return False

        if not reviewer_ids:
            data.set_outputs('result', '')
            data.set_outputs('_error', '审核成员不能为空')
            return False

        # --- look up feishu open_ids for platform users ---
        try:
            from django.contrib.auth import get_user_model
            User = get_user_model()
            users = User.objects.filter(
                id__in=reviewer_ids,
                feishu_openid__isnull=False,
            ).exclude(feishu_openid='')
        except Exception as e:
            logger.exception('FeishuApprovalService: failed to query users')
            data.set_outputs('_error', f'查询用户失败: {e}')
            return False

        reviewer_open_ids = [u.feishu_openid for u in users]
        if not reviewer_open_ids:
            data.set_outputs('_error', '所选审核成员均未绑定飞书账号')
            return False

        # --- create DB record ---
        approval_token = secrets.token_hex(16)

        try:
            from tasks.models import FeishuApprovalRecord
            record = FeishuApprovalRecord.objects.create(
                token=approval_token,
                content=content,
                reviewer_open_ids=reviewer_open_ids,
                callback_node_id=self.id,
                callback_node_version=self.version,
            )
        except Exception as e:
            logger.exception('FeishuApprovalService: failed to create FeishuApprovalRecord')
            data.set_outputs('_error', f'创建审核记录失败: {e}')
            return False

        # --- send personalised card to each reviewer ---
        try:
            access_token = _get_access_token()
        except ValueError as e:
            data.set_outputs('_error', str(e))
            return False

        message_ids = {}
        for open_id in reviewer_open_ids:
            card = _build_approval_card(
                content=content,
                token=approval_token,
                open_id=open_id,
                callback_node_id=self.id,
                callback_node_version=self.version,
            )
            msg_id = _send_card(access_token, open_id, card)
            if msg_id:
                message_ids[open_id] = msg_id

        # Persist message IDs for later card-update
        if message_ids:
            record.message_ids = message_ids
            record.save(update_fields=['message_ids', 'updated_at'])

        logger.info(
            f'FeishuApprovalService: sent cards to {len(reviewer_open_ids)} reviewer(s), '
            f'token={approval_token}'
        )
        return True

    # ------------------------------------------------------------------ #
    # schedule                                                             #
    # ------------------------------------------------------------------ #
    def schedule(self, data, parent_data, callback_data=None):
        if not callback_data:
            data.set_outputs('_error', '缺少审核回调数据')
            return False

        result = callback_data.get('result')
        if result not in ('1', '0'):
            data.set_outputs('_error', '无效的审核回调结果')
            return False

        data.set_outputs('result', result)
        logger.info(
            'FeishuApprovalService: approval callback complete, '
            f'result={result}, token={callback_data.get("token", "")}'
        )
        return True

    # ------------------------------------------------------------------ #
    # inputs / outputs format                                              #
    # ------------------------------------------------------------------ #
    def inputs_format(self):
        return [
            self.InputItem(
                name='审核内容',
                key='content',
                type='string',
                required=True,
            ),
            self.InputItem(
                name='审核成员',
                key='reviewer_ids',
                type='list',
                required=True,
                schema=ExtendedArraySchema(
                    item_schema=StringItemSchema(description='平台用户 ID'),
                    description='选择需要审核的成员（需已绑定飞书账号）',
                    param_type='users',
                ),
            ),
        ]

    def outputs_format(self):
        return [
            self.OutputItem(
                name='审核结果',
                key='result',
                type='string',
            ),
        ]


class FeishuApprovalComponent(Component):
    name = '飞书审核'
    code = 'feishu_approval'
    bound_service = FeishuApprovalService
    version = '1.0'
    category = 'Feishu'
    icon = 'CheckCircle'
    description = (
        '向审核成员发送飞书卡片通知，成员点击通过/不通过后节点完成并输出审核结果。'
        '节点采用回调模式，无需轮询数据库即可等待异步审核。'
    )
