# -*- coding: utf-8 -*-
import io
import re

import demjson
import pandas as pd

from findy.interface import Region, Provider, EntityType
from findy.interface.writer import df_to_db
from findy.database.schema import BlockCategory
from findy.database.schema.meta.stock_meta import EtfStock, Etf
from findy.database.plugins.recorder import RecorderForEntities
from findy.database.context import get_db_session
from findy.database.quote import china_stock_code_to_id
from findy.utils.request import get_http_session, sync_get, chrome_copy_header_to_dict
from findy.utils.time import now_pd_timestamp

DEFAULT_SH_ETF_LIST_HEADER = chrome_copy_header_to_dict('''
Host: query.sse.com.cn
User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36
Accept: */*
Referer: http://www.sse.com.cn/assortment/fund/etf/list/
Accept-Encoding: gzip, deflate
Accept-Language: zh-CN,zh;q=0.9
Cookie: yfx_c_g_u_id_10000042=_ck19062609443812815766114343798; VISITED_COMPANY_CODE=%5B%22510300%22%5D; VISITED_FUND_CODE=%5B%22510300%22%5D; VISITED_MENU=%5B%228307%22%2C%228823%22%2C%228547%22%2C%228556%22%2C%228549%22%2C%2210848%22%2C%228550%22%5D; yfx_f_l_v_t_10000042=f_t_1561513478278__r_t_1561692626758__v_t_1561695738302__r_c_1
Connection: keep-alive
''')


class ChinaETFListSpider(RecorderForEntities):
    region = Region.CHN
    provider = Provider.Exchange
    data_schema = EtfStock

    async def run(self):
        http_session = get_http_session()
        db_session = get_db_session(self.region, self.provider, self.data_schema)
        db_session_etf = get_db_session(self.region, self.provider, Etf)

        # 抓取沪市 ETF 列表
        url = 'http://query.sse.com.cn/commonQuery.do?sqlId=COMMON_SSE_ZQPZ_ETFLB_L_NEW'
        text = sync_get(http_session, url, headers=DEFAULT_SH_ETF_LIST_HEADER, return_type='text')
        if text is None:
            return

        response_dict = demjson.decode(text)

        df = pd.DataFrame(response_dict.get('result', []))
        await self.persist_etf_list(df, 'sh', db_session_etf)
        self.logger.info('沪市 ETF 列表抓取完成...')

        # 抓取沪市 ETF 成分股
        await self.download_sh_etf_component(df, http_session, db_session)
        self.logger.info('沪市 ETF 成分股抓取完成...')

        # 抓取深市 ETF 列表
        url = 'http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1945'
        content = sync_get(http_session, url, return_type='content')
        if content is None:
            return

        df = pd.read_excel(io.BytesIO(content), dtype=str)
        await self.persist_etf_list(df, 'sz', db_session_etf)
        self.logger.info('深市 ETF 列表抓取完成...')

        # 抓取深市 ETF 成分股
        await self.download_sz_etf_component(df, http_session, db_session)
        self.logger.info('深市 ETF 成分股抓取完成...')

    async def persist_etf_list(self, df: pd.DataFrame, exchange: str, db_session):
        if df is None:
            return

        df = df.copy()
        if exchange == 'sh':
            df = df[['FUND_ID', 'FUND_NAME']]
        elif exchange == 'sz':
            df = df[['证券代码', '证券简称']]

        df.columns = ['code', 'name']
        df['id'] = df['code'].apply(lambda code: f'etf_{exchange}_{code}')
        df['entity_id'] = df['id']
        df['exchange'] = exchange
        df['entity_type'] = EntityType.ETF.value
        df['category'] = BlockCategory.etf.value

        df = df.dropna(axis=0, how='any')
        df = df.drop_duplicates(subset='id', keep='last')

        await df_to_db(region=self.region,
                       provider=self.provider,
                       data_schema=Etf,
                       db_session=db_session,
                       df=df)

    async def download_sh_etf_component(self, df: pd.DataFrame, http_session, db_session):
        """
        ETF_CLASS => 1. 单市场 ETF 2.跨市场 ETF 3. 跨境 ETF
                        5. 债券 ETF 6. 黄金 ETF
        :param df: ETF 列表数据
        :return: None
        """
        query_url = 'http://query.sse.com.cn/infodisplay/queryConstituentStockInfo.do?' \
                    'isPagination=false&type={}&etfClass={}'

        etf_df = df[(df['ETF_CLASS'] == '1') | (df['ETF_CLASS'] == '2')]
        etf_df = self.populate_sh_etf_type(etf_df, http_session)

        for _, etf in etf_df.iterrows():
            url = query_url.format(etf['ETF_TYPE'], etf['ETF_CLASS'])
            text = sync_get(http_session, url, headers=DEFAULT_SH_ETF_LIST_HEADER, return_type='text')
            if text is None:
                continue
            response_dict = demjson.decode(text)
            response_df = pd.DataFrame(response_dict.get('result', []))

            etf_code = etf['FUND_ID']
            etf_id = f'etf_sh_{etf_code}'
            response_df = response_df[['instrumentId', 'instrumentName']].copy()
            response_df.rename(columns={'instrumentId': 'stock_code', 'instrumentName': 'stock_name'}, inplace=True)

            response_df['entity_id'] = etf_id
            response_df['entity_type'] = EntityType.ETF.value
            response_df['exchange'] = 'sh'
            response_df['code'] = etf_code
            response_df['name'] = etf['FUND_NAME']
            response_df['timestamp'] = now_pd_timestamp(self.region)

            response_df['stock_id'] = response_df['stock_code'].apply(lambda code: china_stock_code_to_id(code))
            response_df['id'] = response_df['stock_id'].apply(
                lambda x: f'{etf_id}_{x}')

            await df_to_db(region=self.region,
                           provider=self.provider,
                           data_schema=self.data_schema,
                           db_session=db_session,
                           df=response_df)
            self.logger.info(f'{etf["FUND_NAME"]} - {etf_code} 成分股抓取完成...')

    async def download_sz_etf_component(self, df: pd.DataFrame, http_session, db_session):
        query_url = 'http://vip.stock.finance.sina.com.cn/corp/go.php/vII_NewestComponent/indexid/{}.phtml'

        self.parse_sz_etf_underlying_index(df)
        for _, etf in df.iterrows():
            underlying_index = etf['拟合指数']
            etf_code = etf['证券代码']

            if len(underlying_index) == 0:
                self.logger.info(f'{etf["证券简称"]} - {etf_code} 非 A 股市场指数，跳过...')
                continue

            url = query_url.format(underlying_index)
            text = sync_get(http_session, url, encoding='gbk', return_type='text')
            if text is None:
                continue

            try:
                dfs = pd.read_html(text, header=1)
            except ValueError as error:
                self.logger.error(f'HTML parse error: {error}, response: {text}')
                continue

            if len(dfs) < 4:
                continue

            response_df = dfs[3].copy()
            response_df = response_df.dropna(axis=1, how='any')
            response_df['品种代码'] = response_df['品种代码'].apply(lambda x: f'{x:06d}')

            etf_id = f'etf_sz_{etf_code}'
            response_df = response_df[['品种代码', '品种名称']].copy()
            response_df.rename(columns={'品种代码': 'stock_code', '品种名称': 'stock_name'}, inplace=True)

            response_df['entity_id'] = etf_id
            response_df['entity_type'] = EntityType.ETF.value
            response_df['exchange'] = 'sz'
            response_df['code'] = etf_code
            response_df['name'] = etf['证券简称']
            response_df['timestamp'] = now_pd_timestamp(self.region)

            response_df['stock_id'] = response_df['stock_code'].apply(lambda code: china_stock_code_to_id(code))
            response_df['id'] = response_df['stock_id'].apply(
                lambda x: f'{etf_id}_{x}')

            await df_to_db(region=self.region,
                           provider=self.provider,
                           data_schema=self.data_schema,
                           db_session=db_session,
                           df=response_df)
            self.logger.info(f'{etf["证券简称"]} - {etf_code} 成分股抓取完成...')

    def populate_sh_etf_type(self, df: pd.DataFrame, http_session):
        """
        填充沪市 ETF 代码对应的 TYPE 到列表数据中
        :param df: ETF 列表数据
        :return: 包含 ETF 对应 TYPE 的列表数据
        """
        query_url = 'http://query.sse.com.cn/infodisplay/queryETFNewAllInfo.do?' \
                    'isPagination=false&type={}&pageHelp.pageSize=25'

        type_df = pd.DataFrame()
        for etf_class in [1, 2]:
            url = query_url.format(etf_class)
            text = sync_get(http_session, url, headers=DEFAULT_SH_ETF_LIST_HEADER, return_type='text')
            if text is None:
                continue
            response_dict = demjson.decode(text)
            response_df = pd.DataFrame(response_dict.get('result', []))
            response_df = response_df[['fundid1', 'etftype']]

            type_df = pd.concat([type_df, response_df])

        result_df = df.copy()
        result_df = result_df.sort_values(by='FUND_ID').reset_index(drop=True)
        type_df = type_df.sort_values(by='fundid1').reset_index(drop=True)

        result_df['ETF_TYPE'] = type_df['etftype']

        return result_df

    @staticmethod
    def parse_sz_etf_underlying_index(df: pd.DataFrame):
        """
        解析深市 ETF 对应跟踪的指数代码
        :param df: ETF 列表数据
        :return: 解析完成 ETF 对应指数代码的列表数据
        """

        def parse_index(text):
            if len(text) == 0:
                return ''

            result = re.search(r"(\d+).*", text)
            if result is None:
                return ''
            else:
                return result.group(1)

        df['拟合指数'] = df['拟合指数'].apply(parse_index)
