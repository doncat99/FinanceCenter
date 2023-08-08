# -*- coding: utf-8 -*-
import pandas as pd
import tushare as ts

from findy import findy_config
from findy.interface import Region, Provider, EntityType
from findy.database.schema.meta.stock_meta import Stock, StockDetail
from findy.database.recorder import RecorderForEntities
from findy.database.quote import get_entities
from findy.database.persist import df_to_db
from findy.utils.functool import time_it
from findy.utils.time import to_pd_timestamp
from findy.utils.pd import pd_valid

pro = ts.pro_api(findy_config['tushare_token'])


class TushareChinaStockDetailRecorder(RecorderForEntities):
    region = Region.CHN
    provider = Provider.TuShare
    data_schema = StockDetail

    async def init_entities(self, db_session):
        entities, column_names = get_entities(
            region=self.region,
            provider=self.provider,
            db_session=db_session,
            entity_type=EntityType.StockDetail,
            codes=self.codes,
            filters=[
                StockDetail.market_cap == 0,
                StockDetail.sector.is_(None),
                StockDetail.country.is_(None)
            ])
        return entities

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

    @time_it
    async def eval(self, entity, http_session, db_session):
        return False, None

    @time_it
    async def record(self, entity, http_session, db_session, para):
        # get stock info
        info = self.tushare_get_info(entity)

        if pd_valid(info):
            return False, self.format(entity, info)

        return True, None

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

    @time_it
    async def persist(self, entity, http_session, db_session, df_record):
        saved = await df_to_db(region=self.region,
                               provider=self.provider,
                               data_schema=self.data_schema,
                               db_session=db_session,
                               df=df_record,
                               force_update=True)
        return True, saved

    @time_it
    async def on_finish_entity(self, entity, http_session, db_session, result):
        pass

    async def on_finish(self, entities):
        pass
