# -*- coding: utf-8 -*-
from datetime import datetime

import numpy as np
import pandas as pd
from pandas._libs.tslibs.timedeltas import Timedelta

from zvt import zvt_config
from zvt.api.data_type import Region, Provider, EntityType
from zvt.domain import Stock, StockValuation, Etf
from zvt.contract.recorder import TimeSeriesDataRecorder
from zvt.recorders.joinquant.common import to_jq_entity_id
from zvt.networking.request import jq_get_fundamentals
from zvt.utils.time_utils import now_pd_timestamp, to_time_str
from zvt.utils.pd_utils import pd_is_not_null


class JqChinaStockValuationRecorder(TimeSeriesDataRecorder):
    # 数据来自jq
    region = Region.CHN
    provider = Provider.JoinQuant
    entity_schema = Stock
    data_schema = StockValuation

    def __init__(self, entity_type=EntityType.Stock, exchanges=None, entity_ids=None,
                 codes=None, batch_size=10, force_update=False, sleeping_time=5,
                 default_size=zvt_config['batch_size'], real_time=False,
                 fix_duplicate_way='add', start_timestamp=None, end_timestamp=None,
                 close_hour=0, close_minute=0, share_para=None) -> None:
        super().__init__(entity_type, exchanges, entity_ids, codes, batch_size,
                         force_update, sleeping_time, default_size, real_time,
                         fix_duplicate_way, start_timestamp, end_timestamp, close_hour,
                         close_minute, share_para=share_para)

    def get_original_time_field(self):
        return 'day'

    def record(self, entity, start, end, size, timestamps, http_session):
        end = min(now_pd_timestamp(self.region), start + Timedelta(days=500))
        count: Timedelta = end - start

        df = jq_get_fundamentals(table='valuation', code=to_jq_entity_id(entity),
                                 date=to_time_str(end), count=min(count.days, 500))

        if pd_is_not_null(df):
            return df

        return None

    def format(self, entity, df):
        df.rename({'pe_ratio_lyr': 'pe',
                   'pe_ratio': 'pe_ttm',
                   'pb_ratio': 'pb',
                   'ps_ratio': 'ps',
                   'pcf_ratio': 'pcf'}, axis='columns', inplace=True)

        df.update(df.select_dtypes(include=[np.number]).fillna(0))

        df['market_cap'] *= 100000000
        df['circulating_market_cap'] *= 100000000
        df['capitalization'] *= 10000
        df['circulating_cap'] *= 10000
        df['turnover_ratio'] *= 0.01

        if 'timestamp' not in df.columns:
            df['timestamp'] = pd.to_datetime(df[self.get_original_time_field()])
        elif not isinstance(df['timestamp'].dtypes, datetime):
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        df['entity_id'] = entity.id
        df['provider'] = self.provider.value
        df['code'] = entity.code
        df['name'] = entity.name

        df['id'] = self.generate_domain_id(entity, df)
        return df


if __name__ == '__main__':
    # 上证50
    df = Etf.get_stocks(timestamp=now_pd_timestamp(Region.CHN), code='510050')
    stocks = df.stock_id.tolist()
    print(stocks)
    print(len(stocks))

    JqChinaStockValuationRecorder(entity_ids=['stock_sh_600000'], force_update=True).run()


# the __all__ is generated
__all__ = ['JqChinaStockValuationRecorder']
