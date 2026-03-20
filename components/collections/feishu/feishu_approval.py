# -*- coding: utf-8 -*-
"""
Feishu Approval Component

A pipeline node that sends an interactive Feishu card to reviewers with
Approve / Reject buttons, waits for all reviewers to decide, then outputs
the final result ("approved" or "rejected").

Execution flow:
  execute() — send cards and initialize in-node state
  schedule() — consume callback_data repeatedly until all reviewers decide
"""
import json
import logging

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

    execute(): send cards and initialize in-node state.
    schedule(): consume callback_data and finish only when all reviewers decide.
    """

    __need_schedule__ = True
    __multi_callback_enabled__ = True
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
                open_id=open_id,
                callback_node_id=self.id,
                callback_node_version=self.version,
            )
            msg_id = _send_card(access_token, open_id, card)
            if msg_id:
                message_ids[open_id] = msg_id

        # Persist state in node outputs for multi-callback schedule
        data.set_outputs('reviewer_open_ids_json', json.dumps(reviewer_open_ids, ensure_ascii=False))
        data.set_outputs('message_ids_json', json.dumps(message_ids, ensure_ascii=False))
        data.set_outputs('decisions_json', json.dumps({}, ensure_ascii=False))
        data.set_outputs('result', '')

        logger.info(
            f'FeishuApprovalService: sent cards to {len(reviewer_open_ids)} reviewer(s)'
        )
        return True

    # ------------------------------------------------------------------ #
    # schedule                                                             #
    # ------------------------------------------------------------------ #
    def schedule(self, data, parent_data, callback_data=None):
        if not callback_data:
            data.set_outputs('_error', '缺少审核回调数据')
            return False

        action_type = callback_data.get('action_type')
        if action_type != 'feishu_approval':
            data.set_outputs('_error', f'不支持的回调类型: {action_type}')
            return False

        decision = callback_data.get('decision')
        if decision not in ('1', '0'):
            data.set_outputs('_error', f'无效的审核结果: {decision}')
            return False

        open_id_from_value = callback_data.get('open_id', '')
        clicker_open_id = callback_data.get('clicker_open_id', '')
        if clicker_open_id and open_id_from_value and clicker_open_id != open_id_from_value:
            logger.warning(
                'FeishuApprovalService: open_id mismatch '
                f'clicker={clicker_open_id} value={open_id_from_value}'
            )
            return True

        effective_open_id = clicker_open_id or open_id_from_value
        if not effective_open_id:
            data.set_outputs('_error', '审核回调缺少 open_id')
            return False

        try:
            reviewer_open_ids = json.loads(data.get_one_of_outputs('reviewer_open_ids_json', '[]'))
            decisions = json.loads(data.get_one_of_outputs('decisions_json', '{}'))
        except json.JSONDecodeError as e:
            data.set_outputs('_error', f'节点内部状态解析失败: {e}')
            return False

        if effective_open_id not in reviewer_open_ids:
            # Ignore unexpected clickers to keep the node alive.
            logger.warning(
                'FeishuApprovalService: ignore unexpected reviewer '
                f'open_id={effective_open_id}'
            )
            return True

        # Idempotent for duplicate card clicks
        if effective_open_id in decisions:
            return True

        decisions[effective_open_id] = decision
        data.set_outputs('decisions_json', json.dumps(decisions, ensure_ascii=False))

        if set(decisions.keys()) < set(reviewer_open_ids):
            # Wait for more reviewer callbacks
            return True

        result = '0' if any(v == '0' for v in decisions.values()) else '1'
        data.set_outputs('result', result)
        self.finish_schedule()
        logger.info(
            f'FeishuApprovalService: approval callback complete result={result}'
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
            self.OutputItem(
                name='审核消息ID映射(JSON)',
                key='message_ids_json',
                type='string',
            ),
            self.OutputItem(
                name='审核成员(JSON)',
                key='reviewer_open_ids_json',
                type='string',
            ),
            self.OutputItem(
                name='审核决策(JSON)',
                key='decisions_json',
                type='string',
            ),
        ]


class FeishuApprovalComponent(Component):
    name = '审核'
    code = 'feishu_approval'
    bound_service = FeishuApprovalService
    version = '1.0'
    category = 'Feishu'
    icon = 'faUserCheck'
    description = (
        '向审核成员发送飞书卡片通知，成员点击通过/不通过后节点完成并输出审核结果。'
        '节点采用回调模式，无需轮询数据库即可等待异步审核。'
    )
