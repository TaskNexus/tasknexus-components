from pipeline.component_framework.component import Component
from pipeline.core.flow.activity import Service


class Chat2AIService(Service):
    def execute(self, data, parent_data):
        message = data.get_one_of_inputs('message')
        model_group = data.get_one_of_inputs('model_group')
        model_name = data.get_one_of_inputs('model_name')
        project_id = parent_data.get_one_of_inputs('project_id')
        
        if not message or not model_group:
             data.set_outputs('content', 'Message and model group are required')
             return False
             
        try:
            from agents.clients import get_ai_client
            client = get_ai_client(project_id, model_group, use_sdk=True)
            response_text = client.generate_text(message, model=model_name)
            data.set_outputs('content', response_text)
            return True
            
        except Exception as e:
            data.set_outputs('content', f'Error: {str(e)}')
            return False

    def inputs_format(self):
        return [
            self.InputItem(name='Message', key='message', type='string', required=True),
            self.InputItem(name='Model Group', key='model_group', type='string', required=True),
            self.InputItem(name='Model Name', key='model_name', type='string', required=False),
        ]

    def outputs_format(self):
        return [
            self.OutputItem(name='Content', key='content', type='string'),
        ]

class Chat2AI(Component):
    name = 'Chat to AI'
    code = 'chat2ai'
    bound_service = Chat2AIService
    version = '1.0'
    category = 'AI'
