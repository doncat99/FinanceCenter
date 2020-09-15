# -*- coding: utf-8 -*-
import pandas as pd
import pandas_market_calendars as mcal

from zvt.contract.api import df_to_db
from zvt.contract.recorder import TimeSeriesDataRecorder
from zvt.domain import StockTradeDay, Stock
from zvt.contract.common import Region, Provider, EntityType
from zvt.utils.time_utils import to_time_str, now_pd_timestamp, is_datetime


class UsStockTradeDayRecorder(TimeSeriesDataRecorder):
    entity_provider = Provider.Yahoo
    entity_schema = Stock

    provider = Provider.Yahoo
    data_schema = StockTradeDay

    def __init__(self, entity_type=EntityType.Stock, exchanges=['NYSE', 'NASDAQ'], entity_ids=None, codes=None, batch_size=10,
                 force_update=False, sleeping_time=5, default_size=2000, real_time=False, fix_duplicate_way='add',
                 start_timestamp=None, end_timestamp=None, close_hour=0, close_minute=0, share_para=None) -> None:
        super().__init__(entity_type, exchanges, entity_ids, ['A'], batch_size, force_update, sleeping_time,
                         default_size, real_time, fix_duplicate_way, start_timestamp, end_timestamp, close_hour,
                         close_minute, share_para=share_para)
        self.nyse = mcal.get_calendar('NYSE')
        self.nasdaq = mcal.get_calendar('NASDAQ')

    def record(self, entity, start, end, size, timestamps, http_session):
        try:
            trade_day = StockTradeDay.query_data(limit=1, order=StockTradeDay.timestamp.desc(), return_type='domain')
            if len(trade_day) > 0:
                start = trade_day[0].timestamp
        except Exception as _:
            pass

        df = pd.DataFrame()
        dates = self.nyse.schedule(start_date=to_time_str(start), end_date=to_time_str(now_pd_timestamp(Region.US)))
        dates = dates.index.to_list()
        self.logger.info(f'add dates:{dates}')
        df['timestamp'] = pd.to_datetime(dates)
        df['id'] = [to_time_str(date) for date in dates]
        df['entity_id'] = 'nyse'

        df_to_db(df=df, region=Region.US, data_schema=self.data_schema, provider=self.provider, force_update=self.force_update)


__all__ = ['UsStockTradeDayRecorder']

if __name__ == '__main__':
    r = UsStockTradeDayRecorder()
    r.run()
