# -*- coding: utf-8 -*-
"""
Text2Image Component using Google Generative AI (Imagen)
"""

import os
import uuid
import base64
import google
import google.genai as genai
from google.genai import types
from pipeline.component_framework.component import Component
from pipeline.core.flow.activity import Service
from projects.models import Project


class Text2ImageService(Service):
    """
    Generates image from text prompt using Google Generative AI (Imagen).
    Saves image to local storage and returns the file path.
    """
    def execute(self, data, parent_data):
        prompt = data.get_one_of_inputs('prompt')
        model_group = data.get_one_of_inputs('model_group')
        model_name = data.get_one_of_inputs('model_name')
        width = data.get_one_of_inputs('width')
        height = data.get_one_of_inputs('height')
        project_id = parent_data.get_one_of_inputs('project_id')
        
        if not prompt:
            data.set_outputs('image_path', '')
            data.set_outputs('message', 'No prompt provided')
            return False

        if not model_group:
            data.set_outputs('image_path', '')
            data.set_outputs('message', 'No model group specified')
            return False

        if not width or not height:
            data.set_outputs('image_path', '')
            data.set_outputs('message', 'Width and Height are required')
            return False

        try:
            from agents.clients import get_ai_client

            # Delegate generation to AIClient
            # Note: SPLICE variables (e.g. ${var}) are resolved by bamboo-engine before execute()
            client = get_ai_client(project_id, model_group, use_sdk=True)
            image_bytes = client.generate_image(prompt, int(width), int(height), model=model_name)
            
            # Save image to local storage
            output_dir = '/app/media/generated_images'
            os.makedirs(output_dir, exist_ok=True)
            
            filename = f'{uuid.uuid4().hex}.png'
            filepath = os.path.join(output_dir, filename)
            
            with open(filepath, 'wb') as f:
                f.write(image_bytes)
            
            data.set_outputs('image_path', filepath)
            data.set_outputs('message', 'Image generated successfully')
            return True
            
        except Exception as e:
            data.set_outputs('image_path', '')
            data.set_outputs('message', f'Error: {str(e)}')
            return False
            

    def inputs_format(self):
        return [
            self.InputItem(name='Prompt', key='prompt', type='string', required=True),
            self.InputItem(name='Model Group', key='model_group', type='string', required=True),
            self.InputItem(name='Model Name', key='model_name', type='string', required=False),
            self.InputItem(name='Width', key='width', type='string', required=True),
            self.InputItem(name='Height', key='height', type='string', required=True)
        ]

    def outputs_format(self):
        return [
            self.OutputItem(name='Image Path', key='image_path', type='string'),
            self.OutputItem(name='Message', key='message', type='string')
        ]


class Text2ImageComponent(Component):
    name = 'Text to Image'
    code = 'text2image'
    bound_service = Text2ImageService
    version = '1.3'
    category = 'AI'
    description = 'Generate images from text prompts using Google Generative AI (Imagen)'
