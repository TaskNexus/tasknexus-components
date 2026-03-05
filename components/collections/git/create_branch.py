# -*- coding: utf-8 -*-
import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

from pipeline.component_framework.component import Component
from pipeline.core.flow.activity import Service
from pipeline.core.flow.io import (
    StringItemSchema, ArrayItemSchema, ObjectItemSchema,
    IntItemSchema, BooleanItemSchema,
)

logger = logging.getLogger('django')


class GitCreateBranchService(Service):
    """通过 GitLab API 在多个仓库中批量创建分支"""

    def execute(self, data, parent_data):
        source_branch = data.get_one_of_inputs('source_branch')
        target_branch = data.get_one_of_inputs('target_branch')
        repositories = data.get_one_of_inputs('repositories')
        gitlab_url = data.get_one_of_inputs('gitlab_url')
        private_token = data.get_one_of_inputs('private_token')

        if not source_branch:
            data.outputs.ex_data = '基础分支名称不能为空'
            return False

        if not target_branch:
            data.outputs.ex_data = '目标分支名称不能为空'
            return False

        if not repositories:
            data.outputs.ex_data = '仓库列表不能为空'
            return False

        if not gitlab_url:
            data.outputs.ex_data = 'GitLab 地址不能为空'
            return False

        if not private_token:
            data.outputs.ex_data = 'GitLab Private Token 不能为空'
            return False

        gitlab_url = gitlab_url.rstrip('/')
        results = []
        has_failure = False

        def create_branch_for_repo(repo):
            project_id = repo.get('project_id')
            repo_name = repo.get('name', str(project_id))
            try:
                url = f"{gitlab_url}/api/v4/projects/{project_id}/repository/branches"
                headers = {"PRIVATE-TOKEN": private_token}
                payload = {
                    "branch": target_branch,
                    "ref": source_branch,
                }
                resp = requests.post(
                    url, headers=headers, params=payload, timeout=30,
                    proxies={"http": None, "https": None},
                )

                logger.info(
                    '[GitCreateBranch] POST %s -> status=%s, body=%s',
                    url, resp.status_code, resp.text[:500],
                )

                if resp.status_code == 201:
                    branch_info = resp.json()
                    return {
                        'project_id': project_id,
                        'name': repo_name,
                        'success': True,
                        'branch': branch_info.get('name'),
                        'message': '分支创建成功',
                    }
                elif resp.status_code == 400:
                    try:
                        error_msg = resp.json().get('message', '')
                    except (ValueError, AttributeError):
                        error_msg = resp.text[:200]
                    if 'already exists' in str(error_msg):
                        return {
                            'project_id': project_id,
                            'name': repo_name,
                            'success': True,
                            'branch': target_branch,
                            'message': '分支已存在，跳过创建',
                        }
                    return {
                        'project_id': project_id,
                        'name': repo_name,
                        'success': False,
                        'message': f'创建失败({resp.status_code}): {error_msg}',
                    }
                else:
                    try:
                        error_msg = resp.json().get('message', resp.text)
                    except (ValueError, AttributeError):
                        error_msg = resp.text[:200] or f'HTTP {resp.status_code}'
                    return {
                        'project_id': project_id,
                        'name': repo_name,
                        'success': False,
                        'message': f'创建失败({resp.status_code}): {error_msg}',
                    }
            except Exception as e:
                return {
                    'project_id': project_id,
                    'name': repo_name,
                    'success': False,
                    'message': f'请求异常: {str(e)}',
                }

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(create_branch_for_repo, repo): repo
                for repo in repositories
            }
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                if not result['success']:
                    has_failure = True
                    logger.warning(
                        '[GitCreateBranch] 仓库 %s (project_id=%s) 创建分支失败: %s',
                        result['name'], result['project_id'], result['message'],
                    )
                else:
                    logger.info(
                        '[GitCreateBranch] 仓库 %s (project_id=%s) 创建分支成功: %s',
                        result['name'], result['project_id'], result['branch'],
                    )

        data.set_outputs('results', results)
        data.set_outputs('source_branch', source_branch)
        data.set_outputs('target_branch', target_branch)

        if has_failure:
            failed = [r for r in results if not r['success']]
            msgs = '; '.join([f"{r['name']}: {r['message']}" for r in failed])
            data.outputs.ex_data = f'部分仓库创建分支失败: {msgs}'
            return False

        return True

    def inputs_format(self):
        return [
            self.InputItem(
                name='GitLab 地址',
                key='gitlab_url',
                type='string',
                required=True,
                schema=StringItemSchema(description='GitLab 实例地址，例如 https://gitlab.example.com'),
            ),
            self.InputItem(
                name='Private Token',
                key='private_token',
                type='string',
                required=True,
                schema=StringItemSchema(description='GitLab API 访问令牌'),
            ),
            self.InputItem(
                name='仓库列表',
                key='repositories',
                type='array',
                required=True,
                schema=ArrayItemSchema(
                    description='要创建分支的仓库列表，每项包含 project_id 和 name',
                    item_schema=ObjectItemSchema(
                        property_schemas={
                            'project_id': IntItemSchema(description='GitLab 项目 ID'),
                            'name': StringItemSchema(description='仓库名称'),
                        },
                        description='仓库信息',
                    ),
                ),
            ),
            self.InputItem(
                name='基础分支',
                key='source_branch',
                type='string',
                required=True,
                schema=StringItemSchema(description='基于此分支创建新分支，例如 main、master、develop'),
            ),
            self.InputItem(
                name='目标分支',
                key='target_branch',
                type='string',
                required=True,
                schema=StringItemSchema(description='要创建的新分支名称，例如 feature/xxx、release/v1.0'),
            ),
        ]

    def outputs_format(self):
        return [
            self.OutputItem(
                name='创建结果',
                key='results',
                type='array',
                schema=ArrayItemSchema(
                    description='每个仓库的创建结果列表',
                    item_schema=ObjectItemSchema(
                        property_schemas={
                            'project_id': IntItemSchema(description='GitLab 项目 ID'),
                            'name': StringItemSchema(description='仓库名称'),
                            'success': BooleanItemSchema(description='是否创建成功'),
                            'message': StringItemSchema(description='结果信息'),
                        },
                        description='创建结果详情',
                    ),
                ),
            ),
            self.OutputItem(
                name='基础分支',
                key='source_branch',
                type='string',
                schema=StringItemSchema(description='基础分支名称'),
            ),
            self.OutputItem(
                name='目标分支',
                key='target_branch',
                type='string',
                schema=StringItemSchema(description='创建的目标分支名称'),
            ),
        ]


class GitCreateBranchComponent(Component):
    name = 'Git 创建分支'
    code = 'git_create_branch'
    bound_service = GitCreateBranchService
    version = '1.0'
    category = 'Git'
    icon = 'git-pull-request'
    description = '通过 GitLab API 在多个仓库中批量创建分支，仓库地址配置在节点中，输入基础分支和目标分支名称'
