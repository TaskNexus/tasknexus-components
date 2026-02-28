import requests
from django.utils import timezone
from pipeline.component_framework.component import Component
from pipeline.core.flow.activity import Service


class TencentNewsHotService(Service):
    """腾讯新闻热点抓取服务，调用官方热点排行 API 获取实时热点内容。"""

    HOT_API = 'https://r.inews.qq.com/gw/event/hot_ranking_list'

    def execute(self, data, parent_data):
        try:
            max_items = data.get_one_of_inputs('max_items') or 20

            headers = {
                'User-Agent': (
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/120.0.0.0 Safari/537.36'
                ),
            }

            response = requests.get(
                self.HOT_API,
                params={'page_size': int(max_items)},
                headers=headers,
                timeout=30,
            )
            response.raise_for_status()
            result = response.json()

            if result.get('ret') != 0:
                data.set_outputs('content', f'API 返回异常: ret={result.get("ret")}')
                return False

            # 提取新闻列表，跳过无链接的说明条目
            newslist = []
            for idlist_item in result.get('idlist', []):
                for item in idlist_item.get('newslist', []):
                    if not item.get('url') and not item.get('surl'):
                        continue
                    newslist.append(item)

            news_items = newslist[:int(max_items)]

            # 格式化输出
            lines = []
            for item in news_items:
                ranking = item.get('hotEvent', {}).get('ranking', '')
                title = item.get('title', '无标题')
                link = item.get('surl') or item.get('url', '')
                hot_score = item.get('hotEvent', {}).get('hotScore', 0)

                rank_str = f'{ranking}.' if ranking else ''
                hot_str = f' 🔥{hot_score:,}' if hot_score else ''
                lines.append(f'{rank_str}[{title}]({link}){hot_str}')

            data.set_outputs('content', '\n'.join(lines))
            return True

        except requests.RequestException as e:
            data.set_outputs('content', f'网络请求失败: {str(e)}')
            return False
        except Exception as e:
            data.set_outputs('content', f'抓取失败: {str(e)}')
            return False

    def inputs_format(self):
        return [
            self.InputItem(
                name='Max Items',
                key='max_items',
                type='int',
                required=False,
            ),
        ]

    def outputs_format(self):
        return [
            self.OutputItem(name='Content', key='content', type='string'),
        ]


class TencentNewsHotComponent(Component):
    name = 'Tencent News Hot'
    code = 'tencent_news_hot'
    bound_service = TencentNewsHotService
    version = '1.0'
    category = 'News'
    icon = 'Flame'
    description = '腾讯新闻热点排行 - 实时抓取腾讯新闻热点榜单'
