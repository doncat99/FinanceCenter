# -*- coding: utf-8 -*-
import argparse

import pandas as pd

from zvt import init_log, zvt_config
from zvt.api.data_type import Region, Provider, EntityType
from zvt.api.quote import get_kdata, get_kdata_schema
from zvt.domain import Stock, StockKdataCommon, Stock1dKdata
from zvt.contract import IntervalLevel, AdjustType
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.recorders.yahoo.common import to_yahoo_trading_level
from zvt.networking.request import yh_get_bars
from zvt.utils.pd_utils import pd_is_not_null
from zvt.utils.time_utils import to_time_str, PD_TIME_FORMAT_DAY, PD_TIME_FORMAT_ISO8601


class YahooUsStockKdataRecorder(FixedCycleDataRecorder):
    # 数据来自yahoo
    region = Region.US
    provider = Provider.Yahoo
    entity_schema = Stock
    # 只是为了把recorder注册到data_schema
    data_schema = StockKdataCommon

    def __init__(self,
                 exchanges=['nyse', 'nasdaq', 'amex'],
                 entity_ids=None,
                 codes=None,
                 batch_size=10,
                 force_update=True,
                 sleeping_time=0,
                 default_size=zvt_config['batch_size'],
                 real_time=False,
                 fix_duplicate_way='ignore',
                 start_timestamp=None,
                 end_timestamp=None,
                 level=IntervalLevel.LEVEL_1WEEK,
                 kdata_use_begin_time=False,
                 close_hour=15,
                 close_minute=0,
                 one_day_trading_minutes=4 * 60,
                 adjust_type=AdjustType.qfq,
                 share_para=None) -> None:
        level = IntervalLevel(level)
        adjust_type = AdjustType(adjust_type)
        self.data_schema = get_kdata_schema(entity_type=EntityType.Stock, level=level, adjust_type=adjust_type)
        self.yahoo_trading_level = to_yahoo_trading_level(level)

        super().__init__(EntityType.Stock, exchanges, entity_ids, codes, batch_size, force_update, sleeping_time,
                         default_size, real_time, fix_duplicate_way, start_timestamp, end_timestamp, close_hour,
                         close_minute, level, kdata_use_begin_time, one_day_trading_minutes, share_para=share_para)
        self.adjust_type = adjust_type

    def generate_domain_id(self, entity, df, time_fmt=PD_TIME_FORMAT_DAY):
        format = PD_TIME_FORMAT_DAY if self.level >= IntervalLevel.LEVEL_1DAY else PD_TIME_FORMAT_ISO8601
        return entity.id + '_' + df[self.get_evaluated_time_field()].dt.strftime(format)

    def record(self, entity, start, end, size, timestamps, http_session):
        if self.end_timestamp:
            end_timestamp = to_time_str(self.end_timestamp)
            df = yh_get_bars(code=entity.code, interval=self.yahoo_trading_level, start=start, end=end_timestamp, actions=False)
        else:
            end_timestamp = None
            df = yh_get_bars(code=entity.code, interval=self.yahoo_trading_level, start=start, actions=False)

        if pd_is_not_null(df):
            return df
        return None

    def format(self, entity, df):
        df.reset_index(inplace=True)
        df.rename(columns={'Open': 'open', 'High': 'high', 'Low': 'low',
                           'Close': 'close', 'Adj Close': 'adj_close',
                           'Volume': 'volume', 'Date': 'timestamp',
                           'Datetime': 'timestamp'}, inplace=True)

        # df.update(df.select_dtypes(include=[np.number]).fillna(0))

        df['entity_id'] = entity.id
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['provider'] = Provider.Yahoo.value
        df['level'] = self.level.value
        df['code'] = entity.code
        df['name'] = entity.name

        df['id'] = self.generate_domain_id(entity, df)
        return df

    def on_finish(self):
        super().on_finish()


__all__ = ['YahooUsStockKdataRecorder']


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--level', help='trading level', default='1d', choices=[item.value for item in IntervalLevel])
    parser.add_argument('--codes', help='codes', default=['000001'], nargs='+')

    args = parser.parse_args()

    level = IntervalLevel(args.level)
    codes = args.codes

    init_log('yahoo_us_stock_{}_kdata.log'.format(args.level))
    YahooUsStockKdataRecorder(level=level, sleeping_time=0, codes=codes, real_time=False,
                              adjust_type=AdjustType.hfq).run()

    print(get_kdata(region=Region.US, entity_id='stock_nyse_a', limit=10, order=Stock1dKdata.timestamp.desc(),
                    adjust_type=AdjustType.hfq))
