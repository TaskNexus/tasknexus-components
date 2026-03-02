from pipeline.component_framework.component import Component
from pipeline.core.flow.activity import Service


class Chat2AIService(Service):
    def execute(self, data, parent_data):
        message = data.get_one_of_inputs('message')
        model_group = data.get_one_of_inputs('model_group')
        model_name = data.get_one_of_inputs('model_name')
        session_id = data.get_one_of_inputs('session_id')
        project_id = parent_data.get_one_of_inputs('project_id')
        user_id = parent_data.get_one_of_inputs('task_created_by')

        if not message or not model_group:
            data.set_outputs('content', 'Message and model group are required')
            return False

        try:
            from users.models import User
            from agents.services import ChatService

            user = User.objects.get(id=user_id)

            service = ChatService(
                user=user,
                session_id=session_id or None,
                project_id=project_id,
                model_group=model_group,
                model_name=model_name,
                source='pipeline',
            )

            result = service.process_message(
                user_content=message,
            )

            data.set_outputs('content', result.get('result', ''))
            data.set_outputs('session_id', result.get('session_id'))
            return True

        except Exception as e:
            data.set_outputs('content', f'Error: {str(e)}')
            return False

    def inputs_format(self):
        return [
            self.InputItem(name='Message', key='message', type='string', required=True),
            self.InputItem(name='Model Group', key='model_group', type='string', required=True),
            self.InputItem(name='Model Name', key='model_name', type='string', required=False),
            self.InputItem(name='Session ID', key='session_id', type='int', required=False),
        ]

    def outputs_format(self):
        return [
            self.OutputItem(name='Content', key='content', type='string'),
            self.OutputItem(name='Session ID', key='session_id', type='int'),
        ]


class Chat2AI(Component):
    name = 'Chat to AI'
    code = 'chat2ai'
    bound_service = Chat2AIService
    version = '1.0'
    category = 'AI'
