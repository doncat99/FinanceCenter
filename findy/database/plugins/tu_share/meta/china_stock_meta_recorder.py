# -*- coding: utf-8 -*-
import time

import pandas as pd
import tushare as ts

from findy import findy_config
from findy.interface import Region, Provider, EntityType
from findy.interface.writer import df_to_db
from findy.database.schema.meta.stock_meta import StockDetail
from findy.database.plugins.recorder import RecorderForEntities
from findy.utils.time import to_pd_timestamp

pro = ts.pro_api(findy_config['tushare_token'])


class TushareChinaStockDetailRecorder(RecorderForEntities):
    region = Region.CHN
    provider = Provider.TuShare
    data_schema = StockDetail

    async def init_entities(self, db_session):
        self.entities = ['SSE', 'SZSE']

    def generate_domain_id(self, entity, df):
        return df['entity_type'] + '_' + df['exchange'] + '_' + df['code']

    def get_original_time_field(self):
        return 'list_date'

    def tushare_get_info(self, exchange):
        try:
            df_basic = pro.stock_basic(exchange=exchange)
            df_detail = pro.stock_company(exchange=exchange)
            combine_df = pd.merge(df_basic, df_detail, on=['ts_code'])
            # combine_df = df_basic.join(df_detail, on='ts_code')
            return combine_df
        except Exception as e:
            self.logger.error(f'tushare_get_info, code: {exchange}, error: {e}')
        return None

    async def eval(self, entity, http_session, db_session):
        return False, 0, None

    async def record(self, entity, http_session, db_session, para):
        start_point = time.time()

        # get stock info
        info = self.tushare_get_info(entity)

        if len(info) > 0:
            return False, time.time() - start_point, self.format(entity, info)

        return True, time.time() - start_point, None

    def format(self, entity, df):
        df.rename(columns={'symbol': 'code', 'market': 'sector', 'province': 'state', 'employees': 'fulltime_employees',
                           'reg_capital': 'market_cap', 'setup_date': 'date_of_establishment'}, inplace=True)

        timestamp_str = self.get_original_time_field()
        df[timestamp_str] = df[timestamp_str].apply(lambda x: to_pd_timestamp(x))
        df.fillna({timestamp_str: to_pd_timestamp('1980-01-01')}, inplace=True)

        df['timestamp'] = df[timestamp_str]
        df['entity_type'] = EntityType.StockDetail.value
        df['code'] = df['code'].str.strip()
        df['id'] = self.generate_domain_id(entity, df)
        df['entity_id'] = df['id']
        df.drop_duplicates(subset=('id'), keep='last', inplace=True)

        return df

    async def persist(self, entity, http_session, db_session, para):
        start_point = time.time()
        saved = await df_to_db(region=self.region,
                               provider=self.provider,
                               data_schema=self.data_schema,
                               db_session=db_session,
                               df=para,
                               force_update=True)
        return True, time.time() - start_point, saved

    async def on_finish_entity(self, entity, http_session, db_session, result):
        return 0

    async def on_finish(self):
        pass
