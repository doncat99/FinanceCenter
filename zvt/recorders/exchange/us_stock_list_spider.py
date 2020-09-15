# -*- coding: utf-8 -*-

import io

import pandas as pd

from zvt.contract.api import df_to_db
from zvt.contract.recorder import Recorder
from zvt.domain import Stock, StockDetail
from zvt.contract.common import Region, Provider, EntityType
from zvt.recorders.consts import YAHOO_STOCK_LIST_HEADER
from zvt.utils.time_utils import to_pd_timestamp
from zvt.utils.request_utils import get_http_session, request_get


class ExchangeUsStockListRecorder(Recorder):
    data_schema = Stock
    provider = Provider.Exchange

    def run(self):
        http_session = get_http_session()

        exchanges = ['NYSE', 'NASDAQ', 'AMEX']

        for exchange in exchanges:
            url = 'https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&render=download&exchange={}'.format(exchange)
            resp = request_get(http_session, url, headers=YAHOO_STOCK_LIST_HEADER)
            self.download_stock_list(response=resp, exchange=exchange)


    def download_stock_list(self, response, exchange):
        df = pd.read_csv(io.BytesIO(response.content), encoding='UTF8', dtype=str)

        if df is not None:
            df.rename(columns = {'Symbol':'code', 'Name':'name', 'IPOyear':'list_date', 'industry':'industry', 'Sector':'sector'}, inplace = True) 
            df = df[['code', 'name', 'list_date', 'industry', 'sector']]

            df.fillna({'list_date':'1980'}, inplace=True)

            df['list_date'] = df['list_date'].apply(lambda x: to_pd_timestamp(x))
            df['exchange'] = exchange
            df['entity_type'] = EntityType.Stock.value
            df['id'] = df[['entity_type', 'exchange', 'code']].apply(lambda x: '_'.join(x.astype(str)), axis=1)
            df['entity_id'] = df['id'].str.strip()
            df['timestamp'] = df['list_date']
            df = df.dropna(axis=0, how='any')
            df = df.drop_duplicates(subset=('id'), keep='last')

            # persist StockDetail
            df_to_db(df=df, region=Region.US, data_schema=StockDetail, provider=self.provider, force_update=True)

            df.drop(['industry','sector'], axis=1, inplace=True)
            df_to_db(df=df, region=Region.US, data_schema=self.data_schema, provider=self.provider, force_update=True)

            self.logger.info("persist stock list successs")


__all__ = ['ExchangeUsStockListRecorder']

if __name__ == '__main__':
    spider = ExchangeUsStockListRecorder()
    spider.run()
