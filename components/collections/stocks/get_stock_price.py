import re

import requests
from pipeline.component_framework.component import Component
from pipeline.core.flow.activity import Service


class GetStockPriceService(Service):
    """A 股实时股价查询服务，通过腾讯财经 API 获取指定股票的当前价格信息。"""

    API_URL = 'http://qt.gtimg.cn/q={market}{code}'

    # 根据股票代码前缀自动判断市场
    MARKET_RULES = [
        # 上交所：6 开头（主板）、688 开头（科创板）
        (re.compile(r'^6\d{5}$'), 'sh'),
        # 深交所：0 开头（主板）、3 开头（创业板）
        (re.compile(r'^[03]\d{5}$'), 'sz'),
        # 北交所：8 开头、4 开头
        (re.compile(r'^[48]\d{5}$'), 'bj'),
    ]

    def _detect_market(self, code):
        """根据股票代码自动检测所属市场。"""
        for pattern, market in self.MARKET_RULES:
            if pattern.match(code):
                return market
        return None

    def _parse_stock_code(self, raw_code):
        """
        解析用户输入的股票代码，支持以下格式：
        - 纯数字: 600519
        - 带市场前缀: sh600519 / sz000001 / bj830799
        """
        raw_code = raw_code.strip().lower()

        # 带市场前缀的格式
        prefix_match = re.match(r'^(sh|sz|bj)(\d{6})$', raw_code)
        if prefix_match:
            return prefix_match.group(1), prefix_match.group(2)

        # 纯数字格式，自动检测市场
        if re.match(r'^\d{6}$', raw_code):
            market = self._detect_market(raw_code)
            if market:
                return market, raw_code

        return None, raw_code

    def execute(self, data, parent_data):
        try:
            stock_code = data.get_one_of_inputs('stock_code')
            if not stock_code:
                data.set_outputs('content', '请输入股票代码，例如：600519 或 sh600519')
                return False

            market, code = self._parse_stock_code(str(stock_code))
            if not market:
                data.set_outputs(
                    'content',
                    f'无法识别股票代码 "{stock_code}"，请输入 6 位数字代码（如 600519）'
                    f'或带市场前缀的代码（如 sh600519、sz000001）',
                )
                return False

            url = self.API_URL.format(market=market, code=code)
            headers = {
                'User-Agent': (
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/120.0.0.0 Safari/537.36'
                ),
                'Referer': 'https://finance.qq.com/',
            }

            response = requests.get(url, headers=headers, timeout=15)
            response.encoding = 'gbk'
            text = response.text.strip()

            # 响应格式: v_sh600519="1~贵州茅台~600519~1680.00~...";
            match = re.search(r'"(.+)"', text)
            if not match:
                data.set_outputs('content', f'未找到股票 {market}{code} 的行情数据，请检查代码是否正确')
                return False

            fields = match.group(1).split('~')
            if len(fields) < 46 or not fields[3]:
                data.set_outputs('content', f'股票 {market}{code} 数据异常或已退市')
                return False

            # 提取关键字段
            stock_name = fields[1]       # 股票名称
            current_price = fields[3]    # 当前价格
            prev_close = fields[4]       # 昨收价
            open_price = fields[5]       # 开盘价
            volume = fields[6]           # 成交量（手）
            amount = fields[37]          # 成交额（万元）
            high_price = fields[33]      # 最高价
            low_price = fields[34]       # 最低价
            change_amount = fields[31]   # 涨跌额
            change_pct = fields[32]      # 涨跌幅(%)
            trade_date = fields[30]      # 日期
            pe_ratio = fields[39]        # 市盈率
            market_cap = fields[45]      # 总市值（亿）

            # 涨跌 emoji
            try:
                pct_val = float(change_pct)
                if pct_val > 0:
                    trend = '📈'
                    change_color = '🔴'
                elif pct_val < 0:
                    trend = '📉'
                    change_color = '🟢'
                else:
                    trend = '➡️'
                    change_color = '⚪'
            except ValueError:
                trend = ''
                change_color = ''

            # 格式化成交额
            try:
                amount_val = float(amount)
                if amount_val >= 10000:
                    amount_str = f'{amount_val / 10000:.2f} 亿'
                else:
                    amount_str = f'{amount_val:.2f} 万'
            except (ValueError, TypeError):
                amount_str = amount or '-'

            # 格式化总市值
            try:
                market_cap_str = f'{float(market_cap):.2f} 亿'
            except (ValueError, TypeError):
                market_cap_str = market_cap or '-'

            content = (
                f'## {stock_name}（{market.upper()}{code}）{trend}\n\n'
                f'**当前价格**: {change_color} ¥{current_price}\n'
                f'**涨跌额**: {change_amount}  |  **涨跌幅**: {change_pct}%\n\n'
                f'| 指标 | 数值 |\n'
                f'|------|------|\n'
                f'| 开盘价 | ¥{open_price} |\n'
                f'| 昨收价 | ¥{prev_close} |\n'
                f'| 最高价 | ¥{high_price} |\n'
                f'| 最低价 | ¥{low_price} |\n'
                f'| 成交量 | {volume} 手 |\n'
                f'| 成交额 | {amount_str} |\n'
                f'| 市盈率 | {pe_ratio} |\n'
                f'| 总市值 | {market_cap_str} |\n\n'
                f'📅 行情日期：{trade_date}'
            )

            data.set_outputs('content', content)
            data.set_outputs('current_price', current_price)
            return True

        except requests.RequestException as e:
            data.set_outputs('content', f'网络请求失败: {str(e)}')
            return False
        except Exception as e:
            data.set_outputs('content', f'查询失败: {str(e)}')
            return False

    def inputs_format(self):
        return [
            self.InputItem(
                name='Stock Code',
                key='stock_code',
                type='string',
                required=True,
            ),
        ]

    def outputs_format(self):
        return [
            self.OutputItem(name='Content', key='content', type='string'),
            self.OutputItem(name='Current Price', key='current_price', type='string'),
        ]


class GetStockPriceComponent(Component):
    name = 'A Stock Price'
    code = 'get_stock_price'
    bound_service = GetStockPriceService
    version = '1.0'
    category = 'Stocks'
    icon = 'TrendingUp'
    description = 'A 股实时股价查询 - 获取 A 股指定股票的当前价格及行情信息'
