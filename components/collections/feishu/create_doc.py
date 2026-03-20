import logging
import requests
from pipeline.component_framework.component import Component
from pipeline.core.flow.activity import Service

logger = logging.getLogger('django')

class FeishuCreateDocService(Service):
    """
    Service to create a document in Feishu Knowledge Base with multi-level directories.
    """
    
    def _get_tenant_access_token(self):
        from config.models import PlatformConfig
        feishu_config = PlatformConfig.get_feishu_config()
        app_id = feishu_config.get('app_id')
        app_secret = feishu_config.get('app_secret')
        
        if not app_id or not app_secret:
            raise ValueError("Feishu App ID or App Secret is not configured.")
            
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        payload = {
            "app_id": app_id,
            "app_secret": app_secret
        }
        headers = {
            "Content-Type": "application/json; charset=utf-8"
        }
        
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        data = response.json()
        
        if data.get('code') != 0:
            raise ValueError(f"Failed to get tenant_access_token: {data.get('msg')}")
            
        return data.get('tenant_access_token')

    def _get_or_create_folder(self, space_id, name, parent_node_token, access_token):
        """
        Check if a node with the given name exists under parent_node_token.
        If not, create an empty docx node as a directory container.
        Return the node_token of the node.
        
        Note: Feishu Wiki has no dedicated 'folder' type. All nodes are documents
        (docx, doc, sheet, etc.) and any node can have child nodes, forming a tree.
        """
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        
        # 1. Search for existing nodes under parent
        list_url = f"https://open.feishu.cn/open-apis/wiki/v2/spaces/{space_id}/nodes"
        params = {}
        if parent_node_token:
            params['parent_node_token'] = parent_node_token
            
        params['page_size'] = 50 
        
        has_more = True
        page_token = ""
        
        while has_more:
            if page_token:
                params['page_token'] = page_token
                
            response = requests.get(list_url, headers=headers, params=params, timeout=10)
            data = response.json()
            
            if data.get('code') != 0:
                raise ValueError(f"Failed to list nodes: {data.get('msg')}")
                
            items = data.get('data', {}).get('items', [])
            for item in items:
                # Match by title — any node type can serve as a parent container
                if item.get('title') == name:
                    return item.get('node_token')
                    
            has_more = data.get('data', {}).get('has_more', False)
            page_token = data.get('data', {}).get('page_token', '')
            
        # 2. If not found, create an empty docx node as a directory container
        create_url = f"https://open.feishu.cn/open-apis/wiki/v2/spaces/{space_id}/nodes"
        payload = {
            "obj_type": "docx",
            "node_type": "origin",
            "title": name
        }
        if parent_node_token:
            payload['parent_node_token'] = parent_node_token
            
        response = requests.post(create_url, headers=headers, json=payload, timeout=10)
        data = response.json()
        
        if data.get('code') != 0:
            raise ValueError(f"Failed to create directory node '{name}': {data.get('msg')}")
            
        return data.get('data', {}).get('node', {}).get('node_token')
        
    def _find_existing_doc(self, space_id, name, parent_node_token, access_token):
        """
        Search for an existing document with the given name under parent_node_token.
        Returns (node_token, obj_token) if found, (None, None) otherwise.
        """
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        
        list_url = f"https://open.feishu.cn/open-apis/wiki/v2/spaces/{space_id}/nodes"
        params = {'page_size': 50}
        if parent_node_token:
            params['parent_node_token'] = parent_node_token
            
        has_more = True
        page_token = ""
        
        while has_more:
            if page_token:
                params['page_token'] = page_token
                
            response = requests.get(list_url, headers=headers, params=params, timeout=10)
            data = response.json()
            
            if data.get('code') != 0:
                raise ValueError(f"Failed to list nodes: {data.get('msg')}")
                
            items = data.get('data', {}).get('items', [])
            for item in items:
                if item.get('title') == name and item.get('obj_type') == 'docx':
                    return item.get('node_token'), item.get('obj_token')
                    
            has_more = data.get('data', {}).get('has_more', False)
            page_token = data.get('data', {}).get('page_token', '')
            
        return None, None

    def _clear_document_content(self, document_id, access_token):
        """
        Clear all content blocks from an existing document so we can rewrite it.
        """
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        
        # Get all blocks in the document
        list_url = f"https://open.feishu.cn/open-apis/docx/v1/documents/{document_id}/blocks/{document_id}/children"
        params = {'page_size': 500}
        
        response = requests.get(list_url, headers=headers, params=params, timeout=10)
        data = response.json()
        
        if data.get('code') != 0:
            logger.warning(f"Failed to list document blocks for clearing: {data.get('msg')}")
            return
            
        items = data.get('data', {}).get('items', [])
        if not items:
            return
            
        # Delete blocks in reverse order to avoid index shifting
        for block in reversed(items):
            block_id = block.get('block_id')
            if block_id and block_id != document_id:
                delete_url = f"https://open.feishu.cn/open-apis/docx/v1/documents/{document_id}/blocks/{block_id}/children/batch_delete"
                # Actually, we need to delete children of the document root
                pass
        
        # Use batch_delete API to remove all child blocks at once
        child_block_ids = [b.get('block_id') for b in items if b.get('block_id') != document_id]
        if child_block_ids:
            delete_url = f"https://open.feishu.cn/open-apis/docx/v1/documents/{document_id}/blocks/{document_id}/children/batch_delete"
            delete_payload = {
                "start_index": 0,
                "end_index": len(child_block_ids)
            }
            res = requests.delete(delete_url, headers=headers, json=delete_payload, timeout=10)
            res_data = res.json()
            if res_data.get('code') != 0:
                logger.warning(f"Failed to clear document content: {res_data.get('msg')}")
        
    def _create_document(self, space_id, name, parent_node_token, content, access_token):
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        
        # 1. Check if a document with the same name already exists
        node_token, document_id = self._find_existing_doc(space_id, name, parent_node_token, access_token)
        
        if node_token and document_id:
            # Document exists — clear its content and rewrite
            logger.info(f"Found existing document '{name}' (node_token={node_token}), updating content...")
            self._clear_document_content(document_id, access_token)
        else:
            # Create a new wiki node
            create_node_url = f"https://open.feishu.cn/open-apis/wiki/v2/spaces/{space_id}/nodes"
            payload = {
                "obj_type": "docx",
                "node_type": "origin",
                "title": name
            }
            if parent_node_token:
                payload['parent_node_token'] = parent_node_token
                
            response = requests.post(create_node_url, headers=headers, json=payload, timeout=10)
            data = response.json()
            
            if data.get('code') != 0:
                raise ValueError(f"Failed to create document node: {data.get('msg')}")
                
            node_token = data.get('data', {}).get('node', {}).get('node_token')
            document_id = data.get('data', {}).get('node', {}).get('obj_token')
        
        # 2. Write Markdown content to the docx
        if content:
            self._write_content(document_id, content, headers)
                
        return node_token
    
    def _write_content(self, document_id, content, headers):
        """
        Convert Markdown content to Feishu Docx blocks and write them.
        Feishu does NOT have a direct Markdown import API for wiki docs,
        so we parse Markdown locally and create the appropriate block types.
        """
        import re
        
        blocks = []
        lines = content.split('\n')
        i = 0
        
        while i < len(lines):
            line = lines[i]
            stripped = line.strip()
            
            # --- Code block (```) ---
            if stripped.startswith('```'):
                code_lang = stripped[3:].strip() or ''
                code_lines = []
                i += 1
                while i < len(lines) and not lines[i].strip().startswith('```'):
                    code_lines.append(lines[i])
                    i += 1
                if i < len(lines):
                    i += 1  # skip closing ```
                
                code_text = '\n'.join(code_lines)
                if code_text:
                    blocks.append({
                        "block_type": 14,
                        "code": {
                            "style": {
                                "language": self._map_code_language(code_lang)
                            },
                            "elements": [
                                {
                                    "text_run": {
                                        "content": code_text,
                                        "text_element_style": {}
                                    }
                                }
                            ]
                        }
                    })
                continue
            
            # --- Divider (--- or *** or ___) ---
            if re.match(r'^[-*_]{3,}\s*$', stripped):
                blocks.append({
                    "block_type": 22,
                    "divider": {}
                })
                i += 1
                continue
            
            # --- Headings (# to #########) ---
            heading_match = re.match(r'^(#{1,9})\s+(.+)$', stripped)
            if heading_match:
                level = len(heading_match.group(1))
                heading_text = heading_match.group(2).strip()
                block_type = 2 + level  # h1→3, h2→4, ..., h9→11
                field_name = f"heading{level}"
                elements = self._parse_inline(heading_text)
                blocks.append({
                    "block_type": block_type,
                    field_name: {
                        "style": {},
                        "elements": elements
                    }
                })
                i += 1
                continue
            
            # --- Unordered list (- or * or +) ---
            bullet_match = re.match(r'^[-*+]\s+(.+)$', stripped)
            if bullet_match:
                elements = self._parse_inline(bullet_match.group(1))
                blocks.append({
                    "block_type": 12,
                    "bullet": {
                        "style": {},
                        "elements": elements
                    }
                })
                i += 1
                continue
            
            # --- Ordered list (1. 2. etc.) ---
            ordered_match = re.match(r'^\d+[.、]\s+(.+)$', stripped)
            if ordered_match:
                elements = self._parse_inline(ordered_match.group(1))
                blocks.append({
                    "block_type": 13,
                    "ordered": {
                        "style": {},
                        "elements": elements
                    }
                })
                i += 1
                continue
            
            # --- Empty line → empty paragraph ---
            if not stripped:
                blocks.append({
                    "block_type": 2,
                    "text": {
                        "style": {},
                        "elements": [{
                            "text_run": {
                                "content": " ",
                                "text_element_style": {}
                            }
                        }]
                    }
                })
                i += 1
                continue
            
            # --- Regular text paragraph ---
            elements = self._parse_inline(stripped)
            blocks.append({
                "block_type": 2,
                "text": {
                    "style": {},
                    "elements": elements
                }
            })
            i += 1
        
        # Write blocks in batches of 50 (Feishu API limit)
        if not blocks:
            return
            
        write_url = f"https://open.feishu.cn/open-apis/docx/v1/documents/{document_id}/blocks/{document_id}/children"
        batch_size = 50
        logger.info(f"Writing {len(blocks)} blocks to document {document_id} in {(len(blocks) - 1) // batch_size + 1} batches")
        
        for i in range(0, len(blocks), batch_size):
            batch = blocks[i:i + batch_size]
            payload = {
                "children": batch,
                "index": -1
            }
            res = requests.post(write_url, headers=headers, json=payload, timeout=30)
            res_data = res.json()
            if res_data.get('code') != 0:
                logger.error(f"Write batch {i // batch_size + 1} failed: {res_data}")
                raise ValueError(f"Failed to write content: {res_data.get('msg')}")
    
    def _parse_inline(self, text):
        """
        Parse inline Markdown formatting into Feishu text_run elements.
        Supports: **bold**, [link text](url)
        """
        import re
        
        elements = []
        pattern = r'(\*\*(.+?)\*\*|\[([^\]]+)\]\(([^)]+)\))'
        
        last_end = 0
        for match in re.finditer(pattern, text):
            if match.start() > last_end:
                plain = text[last_end:match.start()]
                if plain:
                    elements.append({
                        "text_run": {
                            "content": plain,
                            "text_element_style": {}
                        }
                    })
            
            if match.group(2):
                # **bold**
                elements.append({
                    "text_run": {
                        "content": match.group(2),
                        "text_element_style": {"bold": True}
                    }
                })
            elif match.group(3) and match.group(4):
                # [text](url)
                elements.append({
                    "text_run": {
                        "content": match.group(3),
                        "text_element_style": {
                            "link": {"url": match.group(4)}
                        }
                    }
                })
            
            last_end = match.end()
        
        remaining = text[last_end:]
        if remaining:
            elements.append({
                "text_run": {
                    "content": remaining,
                    "text_element_style": {}
                }
            })
        
        if not elements:
            elements.append({
                "text_run": {
                    "content": " ",
                    "text_element_style": {}
                }
            })
        
        return elements
    
    def _map_code_language(self, lang):
        """Map language identifiers to Feishu code block language codes."""
        lang_map = {
            'python': 49, 'py': 49, 'javascript': 22, 'js': 22,
            'typescript': 67, 'ts': 67, 'java': 21,
            'go': 14, 'golang': 14, 'c': 3, 'cpp': 5, 'c++': 5,
            'csharp': 6, 'c#': 6, 'ruby': 56, 'rust': 57,
            'php': 46, 'swift': 64, 'kotlin': 27, 'scala': 58,
            'sql': 62, 'shell': 59, 'bash': 59, 'sh': 59,
            'html': 18, 'css': 9, 'json': 23,
            'yaml': 75, 'yml': 75, 'xml': 74,
            'markdown': 33, 'md': 33, 'docker': 10, 'dockerfile': 10,
        }
        return lang_map.get(lang.lower(), 1) if lang else 1  # 1 = PlainText

    def execute(self, data, parent_data):
        try:
            space_id = data.get_one_of_inputs('space_id')
            path = data.get_one_of_inputs('path')
            content = data.get_one_of_inputs('content')
            
            if not space_id:
                data.set_outputs('success', False)
                data.set_outputs('error', 'Space ID is required')
                return False
                
            if not path:
                data.set_outputs('success', False)
                data.set_outputs('error', 'Document path is required')
                return False
                
            # Parse path into directories and doc name
            parts = [p.strip() for p in path.split('/') if p.strip()]
            if not parts:
                data.set_outputs('success', False)
                data.set_outputs('error', 'Invalid document path')
                return False
                
            directories = parts[:-1]
            doc_name = parts[-1]
            
            # Fetch access token
            access_token = self._get_tenant_access_token()
            
            # Traverse and create directories
            current_parent_token = ""
            for d in directories:
                current_parent_token = self._get_or_create_folder(space_id, d, current_parent_token, access_token)
                
            # Create final document
            node_token = self._create_document(space_id, doc_name, current_parent_token, content, access_token)
            
            # Note: The domain is usually specific to the tenant, but a generic link can be formed:
            document_url = f"https://feishu.cn/wiki/{node_token}"
            
            data.set_outputs('success', True)
            data.set_outputs('document_url', document_url)
            data.set_outputs('error', '')
            return True
            
        except Exception as e:
            logger.exception("Error in FeishuCreateDocService")
            data.set_outputs('success', False)
            data.set_outputs('error', str(e))
            data.set_outputs('document_url', '')
            return False

    def inputs_format(self):
        return [
            self.InputItem(name='Space ID', key='space_id', type='string', required=True),
            self.InputItem(name='Document Path (e.g. A/B/Doc)', key='path', type='string', required=True),
            self.InputItem(name='Content', key='content', type='string', required=True),
        ]

    def outputs_format(self):
        return [
            self.OutputItem(name='Success', key='success', type='bool'),
            self.OutputItem(name='Document URL', key='document_url', type='string'),
            self.OutputItem(name='Error', key='error', type='string'),
        ]


class FeishuCreateDocComponent(Component):
    name = '知识库文档'
    code = 'feishu_create_doc'
    bound_service = FeishuCreateDocService
    version = '1.0'
    category = 'Feishu'
    icon = 'faFileLines'
    description = '在飞书知识库中创建文档，支持输入多级目录自动创建 (Create Feishu document with multi-level directories)'
