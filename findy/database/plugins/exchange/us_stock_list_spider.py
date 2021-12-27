# -*- coding: utf-8 -*-
import time

import pandas as pd

from findy.interface import Region, Provider, EntityType
from findy.interface.writer import df_to_db
from findy.database.schema.meta.stock_meta import Stock, StockDetail
from findy.database.plugins.recorder import RecorderForEntities
from findy.database.context import get_db_session
from findy.utils.time import to_pd_timestamp

YAHOO_STOCK_LIST_HEADER = {
    'authority': 'old.nasdaq.com',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'sec-fetch-site': 'cross-site',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-user': '?1',
    'sec-fetch-dest': 'document',
    'accept-language': 'en-US,en;q=0.9',
    'cookie': 'AKA_A2=A; NSC_W.TJUFEFGFOEFS.OBTEBR.443=ffffffffc3a0f70e45525d5f4f58455e445a4a42378b',
}


class ExchangeUsStockListRecorder(RecorderForEntities):
    region = Region.US
    provider = Provider.Exchange
    data_schema = Stock

    async def init_entities(self, db_session):
        self.entities = ['nyse', 'nasdaq', 'amex']

    def generate_domain_id(self, entity, df):
        return df['entity_type'] + '_' + df['exchange'] + '_' + df['code']

    def get_original_time_field(self):
        return 'list_date'

    async def process_loop(self, entity, http_session, db_session, throttler):
        url = 'https://api.nasdaq.com/api/screener/stocks'
        params = {'download': 'true', 'exchange': entity}

        try:
            async with http_session.get(url, headers=YAHOO_STOCK_LIST_HEADER, params=params) as response:
                json = await response.json()
                json = json['data']['rows']
                if len(json) > 0:
                    df = self.format(content=json, exchange=entity)
                    await self.persist(df, db_session)
        except Exception as e:
            self.logger.info(f"persist {entity} stock list failed with error: {e}")

    def format(self, content, exchange):
        df = pd.DataFrame(content)

        if df is not None:
            df.rename(columns={'symbol': 'code', 'ipoyear': 'list_date', 'marketCap': 'market_cap'}, inplace=True)

            timestamp_str = self.get_original_time_field()
            df[timestamp_str] = df[timestamp_str].apply(lambda x: to_pd_timestamp(x))
            df.fillna({timestamp_str: to_pd_timestamp('1980-01-01')}, inplace=True)

            df['timestamp'] = df[timestamp_str]
            df['entity_type'] = EntityType.Stock.value
            df['exchange'] = exchange
            df['is_active'] = True
            df['code'] = df['code'].str.strip()
            df['id'] = self.generate_domain_id(exchange, df)
            df['entity_id'] = df['id']
            df.drop_duplicates(subset=('id'), keep='last', inplace=True)

        return df

    async def persist(self, df, db_session):
        start_point = time.time()

        # persist to Stock
        saved = await df_to_db(region=self.region,
                               provider=self.provider,
                               data_schema=self.data_schema,
                               db_session=db_session,
                               df=df,
                               force_update=False)

        # persist to StockDetail
        await df_to_db(region=self.region,
                       provider=self.provider,
                       data_schema=StockDetail,
                       db_session=get_db_session(self.region, self.provider, StockDetail),
                       df=df,
                       force_update=True)

        return True, time.time() - start_point, saved

    async def on_finish(self):
        pass
