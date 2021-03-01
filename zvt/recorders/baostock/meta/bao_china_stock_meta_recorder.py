# -*- coding: utf-8 -*-
import pandas as pd

from zvt.api.data_type import Region, Provider, EntityType
from zvt.domain import Stock, Etf, StockDetail
from zvt.contract.api import df_to_db
from zvt.contract.recorder import Recorder
from zvt.recorders.baostock.common import to_entity_id, to_bao_entity_type
from zvt.networking.request import bao_get_all_securities


class BaseBaoChinaMetaRecorder(Recorder):
    provider = Provider.BaoStock

    def __init__(self, batch_size=10, force_update=True, sleeping_time=10, share_para=None) -> None:
        super().__init__(batch_size, force_update, sleeping_time)

    def to_zvt_entity(self, df, entity_type: EntityType, category=None):
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


class BaoChinaStockRecorder(BaseBaoChinaMetaRecorder):
    data_schema = Stock
    region = Region.CHN

    def run(self):
        # 抓取股票列表
        df_entity = bao_get_all_securities(to_bao_entity_type(EntityType.Stock))
        if not df_entity.empty:
            df_stock = self.to_zvt_entity(df_entity, entity_type=EntityType.Stock)

            df_to_db(df=df_stock, ref_df=None, region=Region.CHN, data_schema=Stock, provider=self.provider)
            # persist StockDetail too
            df_to_db(df=df_stock, ref_df=None, region=Region.CHN, data_schema=StockDetail, provider=self.provider)

            # self.logger.info(df_stock)
            self.logger.info("persist stock list success")


class BaoChinaEtfRecorder(BaseBaoChinaMetaRecorder):
    data_schema = Etf
    region = Region.CHN

    def run(self):
        # 抓取etf列表
        df_index = self.to_zvt_entity(bao_get_all_securities(to_bao_entity_type(EntityType.ETF)), entity_type=EntityType.ETF, category='etf')
        df_to_db(df=df_index, ref_df=None, region=Region.CHN, data_schema=Etf, provider=self.provider)

        # self.logger.info(df_index)
        self.logger.info("persist etf list success")


__all__ = ['BaoChinaStockRecorder', 'BaoChinaEtfRecorder']

if __name__ == '__main__':
    BaoChinaStockRecorder().run()
    # BaoChinaStockEtfPortfolioRecorder(codes=['510050']).run()
