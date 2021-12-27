# -*- coding: utf-8 -*-
import pandas as pd

from findy.interface import Region, Provider, EntityType
from findy.interface.writer import df_to_db
from findy.database.schema.meta.stock_meta import Stock, StockDetail
from findy.database.plugins.recorder import RecorderForEntities
from findy.database.plugins.baostock.common import to_entity_id, to_bao_entity_type
from findy.database.context import get_db_session
from findy.utils.cache import hashable_lru
from findy.utils.pd import pd_valid

import findy.vendor.baostock as bs
try:
    bs.login()
except:
    pass


class BaoChinaStockDetailRecorder(RecorderForEntities):
    region = Region.CHN
    provider = Provider.BaoStock
    data_schema = StockDetail


class BaoChinaStockListRecorder(RecorderForEntities):
    region = Region.CHN
    provider = Provider.BaoStock
    data_schema = Stock

    @hashable_lru
    def bao_get_all_securities(self, entity_type):

        def _bao_get_all_securities(entity_type):
            k_rs = bs.query_stock_basic()
            df = k_rs.get_data()
            return df[df['type'] == entity_type] if not df.empty else df

        try:
            return _bao_get_all_securities(entity_type)
        except Exception as e:
            self.logger.error(f'bao_get_all_securities, error: {e}')
        return None

    def to_entity(self, df, entity_type: EntityType, category=None):
        # 上市日期
        df.rename(columns={'ipoDate': 'list_date', 'outDate': 'end_date', 'code_name': 'name'}, inplace=True)
        df['end_date'].replace(r'^\s*$', '2200-01-01', regex=True, inplace=True)

        df['list_date'] = pd.to_datetime(df['list_date'])
        df['end_date'] = pd.to_datetime(df['end_date'])
        df['timestamp'] = df['list_date']

        df['entity_id'] = df['code'].apply(lambda x: to_entity_id(entity_type=entity_type, bao_code=x))
        df['id'] = df['entity_id']
        df['entity_type'] = entity_type.value
        df[['exchange', 'code']] = df['code'].str.split('.', expand=True)

        if category:
            df['category'] = category

        return df

    async def run(self):
        # 抓取股票列表
        df_entity = self.bao_get_all_securities(to_bao_entity_type(EntityType.Stock))

        if pd_valid(df_entity):
            df_stock = self.to_entity(df_entity, entity_type=EntityType.Stock)

            # persist to Stock
            await df_to_db(region=self.region,
                           provider=self.provider,
                           data_schema=Stock,
                           db_session=get_db_session(self.region, self.provider, Stock),
                           df=df_stock)

            # persist StockDetail too
            await df_to_db(region=self.region,
                           provider=self.provider,
                           data_schema=StockDetail,
                           db_session=get_db_session(self.region, self.provider, StockDetail),
                           df=df_stock)

            self.logger.info("persist stock list success")
