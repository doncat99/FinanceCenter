# -*- coding: utf-8 -*-
import pandas as pd

from zvt.contract.api import df_to_db
from zvt.contract.recorder import TimeSeriesDataRecorder
from zvt.domain import StockTradeDay, Stock
from zvt.contract.common import Region, Provider, EntityType
from zvt.utils.time_utils import to_time_str
from zvt.utils.request_utils import jq_auth, jq_get_trade_days


class ChinaStockTradeDayRecorder(TimeSeriesDataRecorder):
    entity_provider = Provider.JoinQuant
    entity_schema = Stock

    provider = Provider.JoinQuant
    data_schema = StockTradeDay

    def __init__(self, entity_type=EntityType.Stock, exchanges=['sh', 'sz'], entity_ids=None, codes=None, batch_size=10,
                 force_update=False, sleeping_time=5, default_size=2000, real_time=False, fix_duplicate_way='add',
                 start_timestamp=None, end_timestamp=None, close_hour=0, close_minute=0, share_para=None) -> None:
        super().__init__(entity_type, exchanges, entity_ids, ['000001'], batch_size, force_update, sleeping_time,
                         default_size, real_time, fix_duplicate_way, start_timestamp, end_timestamp, close_hour,
                         close_minute, share_para=share_para)
        jq_auth()

    def record(self, entity, start, end, size, timestamps, http_session):
        try:
            trade_day = StockTradeDay.query_data(limit=1, order=StockTradeDay.timestamp.desc(), return_type='domain')
            if len(trade_day) > 0:
                start = trade_day[0].timestamp
        except Exception as _:
            pass

        df = pd.DataFrame()
        dates = jq_get_trade_days(start_date=start)
        self.logger.info(f'add dates:{dates}')
        df['timestamp'] = pd.to_datetime(dates)
        df['id'] = [to_time_str(date) for date in dates]
        df['entity_id'] = 'sz'

        df_to_db(df=df, region=Region.CHN, data_schema=self.data_schema, provider=self.provider, force_update=self.force_update)


__all__ = ['ChinaStockTradeDayRecorder']

if __name__ == '__main__':
    r = ChinaStockTradeDayRecorder()
    r.run()
