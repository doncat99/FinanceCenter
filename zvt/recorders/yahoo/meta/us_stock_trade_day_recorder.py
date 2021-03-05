# -*- coding: utf-8 -*-
from datetime import datetime

import pandas as pd
import pandas_market_calendars as mcal

from zvt.api.data_type import Region, Provider
from zvt.domain import StockTradeDay, Stock
from zvt.contract.recorder import RecorderForEntities
from zvt.contract.api import df_to_db
from zvt.utils.time_utils import to_time_str, now_pd_timestamp, PD_TIME_FORMAT_DAY


class UsStockTradeDayRecorder(RecorderForEntities):
    provider = Provider.Yahoo
    region = Region.US
    entity_schema = Stock
    data_schema = StockTradeDay

    def init_entities(self):
        self.entities = ['NYSE']

    def generate_domain_id(self, entity, df, time_fmt=PD_TIME_FORMAT_DAY):
        return df['timestamp'].dt.strftime(time_fmt)

    def process_loop(self, entity, http_session):
        calendar = mcal.get_calendar(entity)

        trade_days = StockTradeDay.query_data(region=self.region,
                                              return_type='df')
        if len(trade_days) > 0:
            start = to_time_str(trade_days['timestamp'].max(axis=0))
        else:
            start = "1980-01-01"

        dates = calendar.schedule(start_date=start, end_date=to_time_str(now_pd_timestamp(Region.US)))
        dates = dates.index.to_list()
        self.logger.info(f'add dates:{dates}')

        if len(dates) > 0:
            df = pd.DataFrame(dates, columns=['timestamp'])
            df = self.format(entity, df)
            self.persist(df)

        return None

    def format(self, entity, df):
        if 'timestamp' not in df.columns:
            df['timestamp'] = pd.to_datetime(df[self.get_original_time_field()])
        elif not isinstance(df['timestamp'].dtypes, datetime):
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        df['entity_id'] = 'nyse'
        df['provider'] = self.provider.value

        df['id'] = self.generate_domain_id(entity, df)
        return df

    def persist(self, df):
        df_to_db(df=df, ref_df=None, region=self.region, data_schema=self.data_schema, provider=self.provider)

    def on_finish(self):
        pass


__all__ = ['UsStockTradeDayRecorder']


if __name__ == '__main__':
    r = UsStockTradeDayRecorder()
    r.run()