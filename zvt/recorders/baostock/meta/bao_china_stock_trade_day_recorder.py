# -*- coding: utf-8 -*-
import pandas as pd

from zvt.api.data_type import Region, Provider
from zvt.domain import StockTradeDay, Stock
from zvt.contract.recorder import RecorderForEntities
from zvt.networking.request import bao_get_trade_days
from zvt.utils.time_utils import to_time_str, PD_TIME_FORMAT_DAY


class BaoChinaStockTradeDayRecorder(RecorderForEntities):
    region = Region.CHN
    provider = Provider.BaoStock
    entity_schema = Stock
    data_schema = StockTradeDay

    def init_entities(self):
        self.entities = ['stock_sz_000001']

    def generate_domain_id(self, entity, df, time_fmt=PD_TIME_FORMAT_DAY):
        return df['timestamp'].dt.strftime(time_fmt)

    def process_loop(self, entity, http_session):
        trade_days = StockTradeDay.query_data(region=self.region, return_type='df')
        if len(trade_days) > 0:
            start = to_time_str(trade_days['timestamp'].max(axis=0))
        else:
            start = "1990-12-19"
        return bao_get_trade_days(start_date=start)

    def format(self, entity, df):
        df['timestamp'] = pd.to_datetime(df[df['is_trading_day'] == '1']['calendar_date'])
        df['entity_id'] = 'stock_sz_000001'
        df['provider'] = self.provider.value
        df['id'] = self.generate_domain_id(entity, df)
        return df

    def on_finish(self):
        pass


__all__ = ['BaoChinaStockTradeDayRecorder']


if __name__ == '__main__':
    r = BaoChinaStockTradeDayRecorder()
    r.run()
