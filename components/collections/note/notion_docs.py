"""
Notion 工作流组件 - 将文本内容保存到 Notion 笔记
"""
import requests
from pipeline.component_framework.component import Component
from pipeline.core.flow.activity import Service


class NotionSavePageService(Service):
    """
    将文本内容保存到 Notion 页面的服务
    """

    NOTION_API_BASE = "https://api.notion.com/v1"
    NOTION_VERSION = "2022-06-28"

    def execute(self, data, parent_data):
        try:
            # 获取输入参数
            content = data.get_one_of_inputs('content')
            api_key = data.get_one_of_inputs('api_key')
            parent_page_id = data.get_one_of_inputs('parent_page_id')
            database_id = data.get_one_of_inputs('database_id')
            page_title = data.get_one_of_inputs('page_title') or 'Untitled'

            if not api_key:
                data.set_outputs('error', 'Notion API Key is required')
                data.set_outputs('success', False)
                return False

            if not parent_page_id and not database_id:
                data.set_outputs('error', 'Either parent_page_id or database_id is required')
                data.set_outputs('success', False)
                return False

            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "Notion-Version": self.NOTION_VERSION
            }

            # 将内容分割成 blocks (Notion 限制每个 block 最多 2000 字符)
            blocks = self._content_to_blocks(content)

            if database_id:
                # 在 Database 中创建页面
                page_data = {
                    "parent": {"database_id": database_id},
                    "properties": {
                        "title": {
                            "title": [
                                {
                                    "text": {"content": page_title}
                                }
                            ]
                        }
                    },
                    "children": blocks
                }
            else:
                # 在 Page 下创建子页面
                page_data = {
                    "parent": {"page_id": parent_page_id},
                    "properties": {
                        "title": {
                            "title": [
                                {
                                    "text": {"content": page_title}
                                }
                            ]
                        }
                    },
                    "children": blocks
                }

            response = requests.post(
                f"{self.NOTION_API_BASE}/pages",
                headers=headers,
                json=page_data,
                timeout=30
            )

            if response.status_code == 200:
                result = response.json()
                page_id = result.get('id', '')
                page_url = result.get('url', '')
                
                data.set_outputs('success', True)
                data.set_outputs('page_id', page_id)
                data.set_outputs('page_url', page_url)
                data.set_outputs('error', '')
                return True
            else:
                error_msg = response.json().get('message', response.text)
                data.set_outputs('success', False)
                data.set_outputs('page_id', '')
                data.set_outputs('page_url', '')
                data.set_outputs('error', f"Notion API Error: {error_msg}")
                return False

        except requests.exceptions.Timeout:
            data.set_outputs('success', False)
            data.set_outputs('error', 'Request timeout')
            return False
        except requests.exceptions.RequestException as e:
            data.set_outputs('success', False)
            data.set_outputs('error', f"Request error: {str(e)}")
            return False
        except Exception as e:
            data.set_outputs('success', False)
            data.set_outputs('error', f"Unexpected error: {str(e)}")
            return False

    def _content_to_blocks(self, content: str) -> list:
        """
        将文本内容转换为 Notion blocks
        支持 Markdown 格式的标题和换行
        """
        if not content:
            return []

        blocks = []
        lines = content.split('\n')
        current_paragraph = []

        for line in lines:
            stripped_line = line.strip()

            # 检测 Markdown 标题
            if stripped_line.startswith('### '):
                # 先保存之前累积的段落
                if current_paragraph:
                    blocks.extend(self._create_paragraph_blocks('\n'.join(current_paragraph)))
                    current_paragraph = []
                # H3 标题
                blocks.append({
                    "type": "heading_3",
                    "heading_3": {
                        "rich_text": [{"type": "text", "text": {"content": stripped_line[4:]}}]
                    }
                })
            elif stripped_line.startswith('## '):
                if current_paragraph:
                    blocks.extend(self._create_paragraph_blocks('\n'.join(current_paragraph)))
                    current_paragraph = []
                # H2 标题
                blocks.append({
                    "type": "heading_2",
                    "heading_2": {
                        "rich_text": [{"type": "text", "text": {"content": stripped_line[3:]}}]
                    }
                })
            elif stripped_line.startswith('# '):
                if current_paragraph:
                    blocks.extend(self._create_paragraph_blocks('\n'.join(current_paragraph)))
                    current_paragraph = []
                # H1 标题
                blocks.append({
                    "type": "heading_1",
                    "heading_1": {
                        "rich_text": [{"type": "text", "text": {"content": stripped_line[2:]}}]
                    }
                })
            elif stripped_line.startswith('---'):
                if current_paragraph:
                    blocks.extend(self._create_paragraph_blocks('\n'.join(current_paragraph)))
                    current_paragraph = []
                # 分隔线
                blocks.append({"type": "divider", "divider": {}})
            elif stripped_line == '':
                # 空行，保存当前段落
                if current_paragraph:
                    blocks.extend(self._create_paragraph_blocks('\n'.join(current_paragraph)))
                    current_paragraph = []
            else:
                # 普通文本，累积到当前段落
                current_paragraph.append(line)

        # 保存最后的段落
        if current_paragraph:
            blocks.extend(self._create_paragraph_blocks('\n'.join(current_paragraph)))

        return blocks

    def _create_paragraph_blocks(self, text: str) -> list:
        """
        创建段落 blocks，处理超过 2000 字符的内容
        Notion API 限制每个 rich_text 元素最多 2000 字符
        """
        if not text:
            return []

        blocks = []
        # 按 2000 字符分割
        max_length = 2000
        chunks = [text[i:i + max_length] for i in range(0, len(text), max_length)]

        for chunk in chunks:
            blocks.append({
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"type": "text", "text": {"content": chunk}}]
                }
            })

        return blocks

    def inputs_format(self):
        return [
            self.InputItem(name='Content', key='content', type='string', required=True),
            self.InputItem(name='Notion API Key', key='api_key', type='string', required=True),
            self.InputItem(name='Parent Page ID', key='parent_page_id', type='string', required=False),
            self.InputItem(name='Database ID', key='database_id', type='string', required=False),
            self.InputItem(name='Page Title', key='page_title', type='string', required=False),
        ]

    def outputs_format(self):
        return [
            self.OutputItem(name='Success', key='success', type='bool'),
            self.OutputItem(name='Page ID', key='page_id', type='string'),
            self.OutputItem(name='Page URL', key='page_url', type='string'),
            self.OutputItem(name='Error', key='error', type='string'),
        ]


class NotionSavePageComponent(Component):
    """
    Notion 保存页面组件
    """
    name = 'Notion Save Page'
    code = 'notion_save_page'
    bound_service = NotionSavePageService
    version = '1.0'
    category = 'Document'
    description = '将文本内容保存到 Notion 笔记'
    icon = 'book'
