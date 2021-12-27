# -*- coding: utf-8 -*-
import io
import time

import pandas as pd

from findy.interface import Region, Provider, EntityType
from findy.interface.writer import df_to_db
from findy.database.schema.meta.stock_meta import Stock, StockDetail
from findy.database.plugins.recorder import RecorderForEntities
from findy.database.context import get_db_session
from findy.utils.request import chrome_copy_header_to_dict
from findy.utils.pd import pd_valid
from findy.utils.time import to_pd_timestamp


DEFAULT_SH_HEADER = chrome_copy_header_to_dict('''
Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8
Accept-Encoding: gzip, deflate
Accept-Language: en,en-US;q=0.9,zh-TW;q=0.8,zh;q=0.7,zh-CN;q=0.6
Host: query.sse.com.cn
Connection: keep-alive
User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.72 Safari/537.36
Accept: image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8
Referer:http://www.sse.com.cn/assortment/stock/list/share/
Upgrade-Insecure-Requests:1
''')

DEFAULT_SZ_HEADER = chrome_copy_header_to_dict('''
Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8
Accept-Encoding:gzip, deflate, sdch
Accept-Language:zh-CN,zh;q=0.8,en;q=0.6
Connection:keep-alive
Host:www.szse.cn
Referer:http://www.szse.cn/main/marketdata/jypz/colist/
Upgrade-Insecure-Requests:1
User-Agent:Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.76 Mobile Safari/537.36
''')


class ExchangeChinaStockListRecorder(RecorderForEntities):
    region = Region.CHN
    provider = Provider.Exchange
    data_schema = Stock

    category_map_url = {
        'sh': 'http://query.sse.com.cn/security/stock/downloadStockListFile.do?csrcCode=&stockCode=&areaName=&stockType=1',
        'sz': 'http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1110&TABKEY=tab1&random=0.20932135244582617',
    }

    category_map_header = {
        'sh': DEFAULT_SH_HEADER,
        'sz': DEFAULT_SZ_HEADER
    }

    async def init_entities(self, db_session):
        self.entities = ['sh', 'sz']

    def generate_domain_id(self, entity, df):
        return df['entity_type'] + '_' + df['exchange'] + '_' + df['code']

    def get_original_time_field(self):
        return 'list_date'

    async def process_loop(self, entity, http_session, db_session, throttler):
        url = self.category_map_url.get(entity, None)
        if url is None:
            return

        async with throttler:
            async with http_session.get(url, headers=self.category_map_header[entity]) as response:
                if response.status != 200:
                    return

                response = await response.read()

                df = self.format(resp=response, exchange=entity)

                if pd_valid(df):
                    await self.persist(df, db_session)

    def format(self, resp, exchange):
        df = None
        if exchange == 'sh':
            # df = pd.read_excel(io.BytesIO(resp.content), sheet_name='主板A股', dtype=str, parse_dates=['上市日期'])
            df = pd.read_csv(io.BytesIO(resp), sep='\t', encoding='GB2312', dtype=str,
                             parse_dates=['上市日期'])
            if df is not None:
                df.columns = [column.strip() for column in df.columns]
                df = df.loc[:, ['公司代码', '公司简称', '上市日期']]

        elif exchange == 'sz':
            df = pd.read_excel(io.BytesIO(resp), sheet_name='A股列表', dtype=str, parse_dates=['A股上市日期'])
            if df is not None:
                df = df.loc[:, ['A股代码', 'A股简称', 'A股上市日期']]

        if df is not None:
            df.columns = ['code', 'name', 'list_date']

            timestamp_str = self.get_original_time_field()
            # handle the dirty data
            # 600996,贵广网络,2016-12-26,2016-12-26,sh,stock,stock_sh_600996,,次新股,贵州,,
            df.loc[df['code'] == '600996', timestamp_str] = '2016-12-26'
            # print(df[df['list_date'] == '-'])
            df[timestamp_str] = df[timestamp_str].apply(lambda x: to_pd_timestamp(x))
            df.fillna({timestamp_str: to_pd_timestamp('1980-01-01')}, inplace=True)

            df['timestamp'] = df[timestamp_str]
            df['entity_type'] = EntityType.Stock.value
            df['exchange'] = exchange
            df['is_active'] = True
            df['code'] = df['code'].str.strip()
            df['id'] = self.generate_domain_id(exchange, df)
            df['entity_id'] = df['id']
            df = df.drop_duplicates(subset=('id'), keep='last')

        return df

    async def persist(self, df, db_session):
        start_point = time.time()
        # persist to Stock
        saved = await df_to_db(region=self.region,
                               provider=self.provider,
                               data_schema=self.data_schema,
                               db_session=db_session,
                               df=df)

        # persist StockDetail too
        await df_to_db(region=self.region,
                       provider=self.provider,
                       data_schema=StockDetail,
                       db_session=get_db_session(self.region, self.provider, StockDetail),
                       df=df,
                       force_update=True)

        return True, time.time() - start_point, saved

    async def on_finish(self):
        self.logger.info("persist stock list successs")
