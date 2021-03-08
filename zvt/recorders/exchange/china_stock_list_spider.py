# -*- coding: utf-8 -*-

import io

import pandas as pd

from zvt.api.data_type import Region, Provider, EntityType
from zvt.domain import Stock, StockDetail
from zvt.contract.recorder import RecorderForEntities
from zvt.contract.api import df_to_db
from zvt.recorders.consts import DEFAULT_SH_HEADER, DEFAULT_SZ_HEADER
from zvt.networking.request import sync_get
from zvt.utils.time_utils import to_pd_timestamp


class ExchangeChinaStockListRecorder(RecorderForEntities):
    data_schema = Stock

    region = Region.CHN
    provider = Provider.Exchange

    category_map_url = {
        'sh': 'http://query.sse.com.cn/security/stock/downloadStockListFile.do?csrcCode=&stockCode=&areaName=&stockType=1',
        'sz': 'http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1110&TABKEY=tab1&random=0.20932135244582617',
    }

    category_map_header = {
        'sh': DEFAULT_SH_HEADER,
        'sz': DEFAULT_SZ_HEADER
    }

    def init_entities(self):
        # self.entities = [(category, url) for category, url in self.category_map_url.items()]
        self.entities = ['sh', 'sz']

    def process_loop(self, entity, http_session):
        url = self.category_map_url.get(entity, None)
        if url is None:
            return

        resp = sync_get(http_session, url, encoding='GB2312', headers=self.category_map_header[entity])
        if resp.status_code != 200:
            return

        self.format(resp=resp, exchange=entity)

    def format(self, resp, exchange):
        df = None
        if exchange == 'sh':
            # df = pd.read_excel(io.BytesIO(resp.content), sheet_name='主板A股', dtype=str, parse_dates=['上市日期'])
            df = pd.read_csv(io.BytesIO(content), sep='\t', encoding='GB2312', dtype=str,
                             parse_dates=['上市日期'])
            if df is not None:
                df.columns = [column.strip() for column in df.columns]
                df = df.loc[:, ['公司代码', '公司简称', '上市日期']]

        elif exchange == 'sz':
            df = pd.read_excel(io.BytesIO(resp.content), sheet_name='A股列表', dtype=str, parse_dates=['A股上市日期'])
            if df is not None:
                df = df.loc[:, ['A股代码', 'A股简称', 'A股上市日期']]

        if df is not None:
            df.columns = ['code', 'name', 'list_date']
            df = df.dropna(subset=['code'])

            # handle the dirty data
            # 600996,贵广网络,2016-12-26,2016-12-26,sh,stock,stock_sh_600996,,次新股,贵州,,
            df.loc[df['code'] == '600996', 'list_date'] = '2016-12-26'
            # print(df[df['list_date'] == '-'])
            df['list_date'] = df['list_date'].apply(lambda x: to_pd_timestamp(x))
            df['exchange'] = exchange
            df['entity_type'] = EntityType.Stock.value
            df['id'] = df[['entity_type', 'exchange', 'code']].apply(lambda x: '_'.join(x.astype(str)), axis=1)
            df['entity_id'] = df['id']
            df['timestamp'] = df['list_date']
            df = df.dropna(axis=0, how='any')
            df = df.drop_duplicates(subset=('id'), keep='last')
            df_to_db(df=df, ref_df=None, region=Region.CHN, data_schema=self.data_schema, provider=self.provider)
            # persist StockDetail too
            df_to_db(df=df, ref_df=None, region=Region.CHN, data_schema=StockDetail, provider=self.provider)
            # self.logger.info(df.tail())
            self.logger.info("persist stock list successs")

    def on_finish(self):
        pass


__all__ = ['ExchangeChinaStockListRecorder']

if __name__ == '__main__':
    spider = ExchangeChinaStockListRecorder()
    spider.run()
