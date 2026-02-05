import requests
from bs4 import BeautifulSoup
from django.utils import timezone
from pipeline.component_framework.component import Component
from pipeline.core.flow.activity import Service

class CCTVNewsService(Service):

    def execute(self, data, parent_data):
        try:
            date_str = data.get_one_of_inputs('date')
            
            # Use headers to mimic browser request
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
                "Referer": "http://tv.cctv.com/"
            }

            # 1. Get News List
            list_url = f"http://tv.cctv.com/lm/xwlb/day/{date_str}.shtml"
            response = requests.get(list_url, headers=headers)
            response.encoding = 'utf-8' # Ensure correct encoding
            
            # The returned content is an HTML fragment, wrap it to parse
            full_html = f"<!DOCTYPE html><html><head></head><body>{response.text}</body></html>"
            soup = BeautifulSoup(full_html, 'html.parser')
            
            links = []
            for a in soup.find_all('a'):
                href = a.get('href')
                if href and href not in links:
                    links.append(href)
            
            if not links:
                data.set_outputs('content', "No news found for this date.")
                return True

            # The first link is typically the abstract/summary page
            abstract_link = links.pop(0)
            
            # 2. Get Abstract
            abstract_text = ""
            try:
                abs_resp = requests.get(abstract_link, headers=headers)
                abs_resp.encoding = 'utf-8'
                abs_soup = BeautifulSoup(abs_resp.text, 'html.parser')
                # Selector from JS: #page_body > div.allcontent > div.video18847 > div.playingCon > div.nrjianjie_shadow > div > ul > li:nth-child(1) > p
                # Simplified selector
                abs_p = abs_soup.select_one('.nrjianjie_shadow ul li p')
                if abs_p:
                    abstract_text = abs_p.get_text().strip()
                    abstract_text = abstract_text.replace('；', "；\n\n").replace('：', "：\n\n")
            except Exception as e:
                print(f"Failed to fetch abstract: {e}")

            # 3. Get News Details
            news_items = []
            for link in links:
                try:
                    news_resp = requests.get(link, headers=headers)
                    news_resp.encoding = 'utf-8'
                    news_soup = BeautifulSoup(news_resp.text, 'html.parser')
                    
                    # Title selector: #page_body ... .tit
                    title_div = news_soup.select_one('.playingVideo .tit')
                    title = title_div.get_text().strip().replace('[视频]', '') if title_div else "No Title"
                    
                    # Content selector: #content_area
                    content_div = news_soup.select_one('#content_area')
                    content = content_div.get_text().strip() if content_div else ""
                    
                    news_items.append({
                        'title': title,
                        'content': content,
                        'link': link
                    })
                except Exception as e:
                    print(f"Failed to fetch news item {link}: {e}")

            # 4. Format Output
            md_news = ""
            for item in news_items:
                md_news += f"### {item['title']}\n\n{item['content']}\n\n[查看原文]({item['link']})\n\n"
            
            final_content = f"# 《新闻联播》 ({date_str})\n\n## 新闻摘要\n\n{abstract_text}\n\n## 详细新闻\n\n{md_news}\n\n---\n\n(更新时间戳: {timezone.now().timestamp()})\n\n"

            data.set_outputs('content', final_content)
            return True
        except Exception as e:
            data.set_outputs('content', f"Error: {str(e)}")
            return False

    def inputs_format(self):
        return [
            self.InputItem(name='Date', key='date', type='string', required=True)
        ]

    def outputs_format(self):
        return [
            self.OutputItem(name='Content', key='content', type='string')
        ]


class CCTVNewsComponent(Component):
    name = 'CCTV News'
    code = 'cctv_news'
    bound_service = CCTVNewsService
    version = '1.0'
    category = 'News'
    description = '每日新闻联播'
    icon = 'newspaper'

