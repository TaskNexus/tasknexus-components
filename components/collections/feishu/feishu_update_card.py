# -*- coding: utf-8 -*-
"""Feishu card update component.

Update one or multiple Feishu interactive cards by message_id map.
Designed to be placed after `feishu_approval` to show final status.
"""
import json
import logging

import requests

from pipeline.component_framework.component import Component
from pipeline.core.flow.activity import Service

logger = logging.getLogger('django')

FEISHU_BASE_URL = 'https://open.feishu.cn/open-apis'


def _get_access_token() -> str:
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
        raise ValueError(f'Failed to get Feishu access token: {data.get("msg")}')
    return data['tenant_access_token']


def _build_status_card(result: str, approved_text: str, rejected_text: str) -> dict:
    approved = result == '1'
    text = approved_text if approved else rejected_text
    return {
        'schema': '2.0',
        'config': {'update_multi': True},
        'header': {
            'title': {'tag': 'plain_text', 'content': '📋 审核结果'},
            'template': 'green' if approved else 'red',
        },
        'body': {
            'elements': [
                {
                    'tag': 'markdown',
                    'content': text,
                }
            ]
        },
    }


def _patch_card(access_token: str, message_id: str, card: dict) -> bool:
    card_json = json.dumps(card, ensure_ascii=False)
    resp = requests.patch(
        f'{FEISHU_BASE_URL}/im/v1/messages/{message_id}',
        headers={
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json; charset=utf-8',
        },
        json={
            'msg_type': 'interactive',
            'content': card_json,
        },
        timeout=10,
    )
    data = resp.json()
    if data.get('code') != 0:
        logger.warning(f'FeishuUpdateCardService: update failed msg_id={message_id} err={data.get("msg")}')
        return False
    return True


class FeishuUpdateCardService(Service):
    """Patch Feishu cards based on message_ids_json."""

    def execute(self, data, parent_data):
        message_ids_json = data.get_one_of_inputs('message_ids_json', '')
        result = str(data.get_one_of_inputs('result', '')).strip()
        approved_text = data.get_one_of_inputs('approved_text', '审核通过 ✅')
        rejected_text = data.get_one_of_inputs('rejected_text', '审核未通过 ❌')

        if result not in ('1', '0'):
            data.set_outputs('success', False)
            data.set_outputs('error', f'无效审核结果: {result}')
            data.set_outputs('updated_count', 0)
            data.set_outputs('failed_count', 0)
            return False

        try:
            message_ids = json.loads(message_ids_json or '{}')
            if not isinstance(message_ids, dict):
                raise ValueError('message_ids_json must be a JSON object')
        except Exception as e:
            data.set_outputs('success', False)
            data.set_outputs('error', f'message_ids_json 解析失败: {e}')
            data.set_outputs('updated_count', 0)
            data.set_outputs('failed_count', 0)
            return False

        if not message_ids:
            data.set_outputs('success', True)
            data.set_outputs('error', '')
            data.set_outputs('updated_count', 0)
            data.set_outputs('failed_count', 0)
            return True

        try:
            access_token = _get_access_token()
        except Exception as e:
            data.set_outputs('success', False)
            data.set_outputs('error', str(e))
            data.set_outputs('updated_count', 0)
            data.set_outputs('failed_count', len(message_ids))
            return False

        card = _build_status_card(result, approved_text, rejected_text)
        updated = 0
        failed = 0
        for _, message_id in message_ids.items():
            if message_id and _patch_card(access_token, message_id, card):
                updated += 1
            else:
                failed += 1

        data.set_outputs('updated_count', updated)
        data.set_outputs('failed_count', failed)
        data.set_outputs('success', failed == 0)
        data.set_outputs('error', '' if failed == 0 else f'部分卡片更新失败: {failed}')
        return failed == 0

    def inputs_format(self):
        return [
            self.InputItem(name='消息ID映射(JSON)', key='message_ids_json', type='string', required=True),
            self.InputItem(name='审核结果(1/0)', key='result', type='string', required=True),
            self.InputItem(name='通过文案', key='approved_text', type='string', required=False),
            self.InputItem(name='不通过文案', key='rejected_text', type='string', required=False),
        ]

    def outputs_format(self):
        return [
            self.OutputItem(name='是否成功', key='success', type='bool'),
            self.OutputItem(name='更新成功数量', key='updated_count', type='int'),
            self.OutputItem(name='更新失败数量', key='failed_count', type='int'),
            self.OutputItem(name='错误信息', key='error', type='string'),
        ]


class FeishuUpdateCardComponent(Component):
    name = '消息卡片更新'
    code = 'feishu_update_card'
    bound_service = FeishuUpdateCardService
    version = '1.0'
    category = 'Feishu'
    icon = 'faArrowsRotate'
    description = '根据 message_id 更新飞书交互卡片状态，通常接在飞书审核节点后执行。'

