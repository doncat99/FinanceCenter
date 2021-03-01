# -*- coding: utf-8 -*-
from datetime import datetime

import pandas as pd

from zvt.api.data_type import Region, Provider
from zvt.domain import StockTradeDay, Stock
from zvt.contract.recorder import RecorderForEntities
from zvt.networking.request import jq_get_trade_days
from zvt.utils.time_utils import to_time_str, PD_TIME_FORMAT_DAY


class StockTradeDayRecorder(RecorderForEntities):
    region = Region.CHN
    provider = Provider.JoinQuant
    entity_schema = Stock
    data_schema = StockTradeDay

    def init_entities(self):
        self.entities = ['stock_sz_000001']

    def generate_domain_id(self, entity, df, time_fmt=PD_TIME_FORMAT_DAY):
        return df['timestamp'].dt.strftime(time_fmt)

    def process_loop(self, entity, http_session):
        trade_days = StockTradeDay.query_data(region=self.region,
                                              return_type='df')
        if len(trade_days) > 0:
            start = to_time_str(trade_days['timestamp'].max(axis=0))
        else:
            start = "1980-01-01"

        dates = jq_get_trade_days(date=start)
        dates = dates.iloc[:, 0]
        self.logger.info(f'add dates:{dates}')

        if len(dates) > 0:
            df = pd.DataFrame(dates, columns=['timestamp'])
            return self.format(df)

        return None

    def format(self, entity, df):
        if 'timestamp' not in df.columns:
            df['timestamp'] = pd.to_datetime(df[self.get_original_time_field()])
        elif not isinstance(df['timestamp'].dtypes, datetime):
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        df['entity_id'] = 'stock_sz_000001'
        df['provider'] = self.provider.value

        df['id'] = self.generate_domain_id(entity, df)
        return df

    def on_finish(self):
        pass


# the __all__ is generated
__all__ = ['StockTradeDayRecorder']


if __name__ == '__main__':
    r = StockTradeDayRecorder()
    r.run()