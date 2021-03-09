# -*- coding: utf-8 -*-
import pandas as pd

from zvt.api.data_type import Region, Provider, EntityType
from zvt.domain import Stock, StockDetail
from zvt.recorders.consts import YAHOO_STOCK_LIST_HEADER
from zvt.contract.recorder import RecorderForEntities
from zvt.contract.api import df_to_db
from zvt.networking.request import sync_get
from zvt.utils.time_utils import to_pd_timestamp


class ExchangeUsStockListRecorder(RecorderForEntities):
    region = Region.CHN
    provider = Provider.Exchange
    data_schema = Stock

    def init_entities(self):
        self.entities = ['nyse', 'nasdaq', 'amex']

    def generate_domain_id(self, entity, df):
        return df['entity_type'] + '_' + df['exchange'] + '_' + df['code']

    def get_original_time_field(self):
        return 'list_date'

    def process_loop(self, entity, http_session):
        url = 'https://api.nasdaq.com/api/screener/stocks'
        params = {'download': 'true', 'exchange': entity}
        resp = sync_get(http_session, url, headers=YAHOO_STOCK_LIST_HEADER, params=params, enable_proxy=False)
        if resp is None:
            return

        json = resp.json()['data']['rows']

        if len(json) > 0:
            df = self.format(content=json, exchange=entity)
            self.persist(df)

        return None

    def format(self, content, exchange):
        df = pd.DataFrame(content)

        if df is not None:
            df.rename(columns={'symbol': 'code', 'ipoyear': 'list_date', 'marketCap': 'market_cap'}, inplace=True)

            timestamp_str = self.get_original_time_field()
            df.fillna({timestamp_str: '1980'}, inplace=True)
            df[timestamp_str] = df[timestamp_str].apply(lambda x: to_pd_timestamp(x))

            df['entity_type'] = EntityType.Stock.value
            df['exchange'] = exchange
            df['code'] = df['code'].str.strip()
            df['id'] = self.generate_domain_id(exchange, df)
            df['entity_id'] = df['id']
            df.drop_duplicates(subset=('id'), keep='last', inplace=True)

        return df

    def persist(self, df):
        # persist to Stock
        df_to_db(df=df, ref_df=None, region=Region.US, data_schema=self.data_schema, provider=self.provider, force_update=True)

        # persist to StockDetail
        df_to_db(df=df, ref_df=None, region=Region.US, data_schema=StockDetail, provider=self.provider, force_update=True)

    def on_finish(self):
        self.logger.info("persist stock list successs")


__all__ = ['ExchangeUsStockListRecorder']

if __name__ == '__main__':
    spider = ExchangeUsStockListRecorder()
    spider.run()
