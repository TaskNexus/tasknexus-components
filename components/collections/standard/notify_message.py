import logging
from pipeline.component_framework.component import Component
from pipeline.core.flow.activity import Service
from pipeline.core.flow.io import StringItemSchema
from components.schemas import ExtendedArraySchema

logger = logging.getLogger('django')


class NotifyMessageService(Service):
    def execute(self, data, parent_data):
        content = data.get_one_of_inputs('content')
        platform = data.get_one_of_inputs('platform', 'telegram')
        user_ids = data.get_one_of_inputs('user_ids', [])
        
        # Validate inputs
        if not content:
            data.set_outputs('message', 'Notification content is required')
            return False
        
        if not user_ids:
            data.set_outputs('message', 'No user_ids specified')
            return False
        
        platform = str(platform).lower().strip()
        
        if platform == 'telegram':
            return self._send_telegram(data, content, user_ids)
        elif platform == 'feishu':
            data.set_outputs('message', 'Feishu notification is not yet implemented')
            return False
        else:
            data.set_outputs('message', f'Unknown notification platform: {platform}')
            return False
    
    def _send_telegram(self, data, content, user_ids):
        from agents.telegram import TelegramService
        
        service = TelegramService()
        result = service.send_message_to_users(content=content, user_ids=user_ids)
        
        success_count = result['success_count']
        total = len(user_ids)
        
        # Set outputs
        if success_count == total:
            data.set_outputs('message', f'Successfully sent to all {total} recipients')
            data.set_outputs('success', True)
        elif success_count > 0:
            data.set_outputs('message', f'Sent to {success_count}/{total} recipients. Errors: {"; ".join(result["errors"])}')
            data.set_outputs('success', True)
        else:
            data.set_outputs('message', f'Failed to send to all recipients. Errors: {"; ".join(result["errors"])}')
            data.set_outputs('success', False)
        return success_count > 0

    def inputs_format(self):
        return [
            self.InputItem(name='Content', key='content', type='string', required=True),
            self.InputItem(name='Platform', key='platform', type='string', required=True),
            self.InputItem(
                name='User IDs', 
                key='user_ids', 
                type='list', 
                required=True,
                schema=ExtendedArraySchema(
                    item_schema=StringItemSchema(description='User ID'),
                    description='Select project members to notify',
                    param_type='users'
                )
            ),
        ]

    def outputs_format(self):
        return [
            self.OutputItem(name='Success', key='success', type='bool'),
            self.OutputItem(name='Message', key='message', type='string'),
        ]


class NotifyMessageComponent(Component):
    name = 'Notify Message'
    code = 'notify_message'
    bound_service = NotifyMessageService
    version = '1.0'
    category = 'Standard'
    description = 'Send notification messages to platform users via Telegram, Feishu, etc.'

