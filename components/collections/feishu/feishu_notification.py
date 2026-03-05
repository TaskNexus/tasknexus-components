import logging
from pipeline.component_framework.component import Component
from pipeline.core.flow.activity import Service
from pipeline.core.flow.io import StringItemSchema
from components.schemas import ExtendedArraySchema

logger = logging.getLogger('django')


class FeishuNotificationService(Service):
    def execute(self, data, parent_data):
        content = data.get_one_of_inputs('content')
        user_ids = data.get_one_of_inputs('user_ids', [])
        
        # Validate inputs
        if not content:
            data.set_outputs('message', 'Notification content is required')
            return False
        
        if not user_ids:
            data.set_outputs('message', 'No user_ids specified')
            return False
        
        # Send via Feishu
        from tasks.notifications import send_feishu_message
        
        result = send_feishu_message(content=content, user_ids=user_ids)
        
        success_count = result['success_count']
        total = result['total']
        
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


class FeishuNotificationComponent(Component):
    name = '飞书通知'
    code = 'feishu_notification'
    bound_service = FeishuNotificationService
    version = '1.0'
    category = 'Feishu'
    icon = 'Bell'
    description = '发送飞书通知'
