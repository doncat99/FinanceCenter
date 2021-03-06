# -*- coding: utf-8 -*-
from datetime import datetime

import pandas as pd
from numba import njit

from zvt.api.data_type import Region, Provider, EntityType
from zvt.api.quote import get_kdata_schema
from zvt.domain import Index, BlockCategory, Block
from zvt.contract import IntervalLevel
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.contract.api import get_entities
from zvt.networking.request import sync_get
from zvt.utils.time_utils import now_time_str, TIME_FORMAT_DAY1, PD_TIME_FORMAT_DAY, PD_TIME_FORMAT_ISO8601
from zvt.utils.utils import json_callback_param, to_float


def level_flag(level: IntervalLevel):
    level = IntervalLevel(level)
    if level == IntervalLevel.LEVEL_1DAY:
        return 101
    if level == IntervalLevel.LEVEL_1WEEK:
        return 102
    if level == IntervalLevel.LEVEL_1MON:
        return 103

    assert False


# 抓取行业的日线,周线,月线数据，用于中期选行业
class ChinaStockKdataRecorder(FixedCycleDataRecorder):
    region = Region.CHN
    provider = Provider.EastMoney
    entity_schema = Block


    url = 'https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=90.{}&cb=fsdata1567673076&klt={}&fqt=0&lmt={}&end={}&iscca=1&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57&ut=f057cbcbce2a86e2866ab8877db1d059&forcect=1&fsdata1567673076=fsdata1567673076'

    def __init__(self, entity_type=EntityType.Index, exchanges=None, entity_ids=None,
                 codes=None, batch_size=10, force_update=False, sleeping_time=10,
                 default_size=10000, real_time=True, fix_duplicate_way='add',
                 start_timestamp=None, end_timestamp=None,
                 level=IntervalLevel.LEVEL_1WEEK, kdata_use_begin_time=False,
                 close_hour=0, close_minute=0, one_day_trading_minutes=24 * 60) -> None:
        self.data_schema = get_kdata_schema(entity_type=entity_type, level=level)
        super().__init__(entity_type, exchanges, entity_ids, codes, batch_size, force_update, sleeping_time,
                         default_size, real_time, fix_duplicate_way, start_timestamp, end_timestamp, close_hour,
                         close_minute, level, kdata_use_begin_time,
                         one_day_trading_minutes)

    def init_entities(self):
        assert self.region is not None

        self.entities = get_entities(region=self.region,
                                     entity_type=EntityType.Index,
                                     exchanges=self.exchanges,
                                     codes=self.codes,
                                     entity_ids=self.entity_ids,
                                     return_type='domain',
                                     provider=self.provider,
                                     # 只抓概念和行业
                                     filters=[Index.category.in_(
                                         [BlockCategory.industry.value, BlockCategory.concept.value])])

    def generate_domain_id(self, entity, df, time_fmt=PD_TIME_FORMAT_DAY):
        format = PD_TIME_FORMAT_DAY if self.level >= IntervalLevel.LEVEL_1DAY else PD_TIME_FORMAT_ISO8601
        return entity.id + '_' + df[self.get_evaluated_time_field()].dt.strftime(format)

    def record(self, entity, start, end, size, timestamps, http_session):
        url = self.url.format("{}".format(entity.code), level_flag(self.level), size,
                              now_time_str(region=Region.CHN, fmt=TIME_FORMAT_DAY1))
        text = sync_get(http_session, url, return_type='text')
        if text is None:
            return None

        results = json_callback_param(text)

        if results:
            klines = results['data']['klines']

            @njit(nopython=True)
            def numba_boost_up(klines):
                kdatas = []
                # TODO: ignore the last unfinished kdata now,could control it better if need
                for result in klines[:-1]:
                    # "2000-01-28,1005.26,1012.56,1173.12,982.13,3023326,3075552000.00"
                    # time,open,close,high,low,volume,turnover
                    fields = result.split(',')
                    kdatas.append(dict(
                                    timestamp=fields[0],
                                    open=to_float(fields[1]),
                                    close=to_float(fields[2]),
                                    high=to_float(fields[3]),
                                    low=to_float(fields[4]),
                                    volume=to_float(fields[5]),
                                    turnover=to_float(fields[6])))
                return kdatas

            kdatas = numba_boost_up(klines)
            if len(kdatas) > 0:
                df = pd.DataFrame.from_records(kdatas)
                return df
        return None

    def format(self, entity, df):
        if 'timestamp' not in df.columns:
            df['timestamp'] = pd.to_datetime(df[self.get_original_time_field()])
        elif not isinstance(df['timestamp'].dtypes, datetime):
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        df['entity_id'] = entity.id
        df['provider'] = self.provider.value
        df['code'] = entity.code
        df['level'] = self.level.value

        df['id'] = self.generate_domain_id(entity, df)
        return df


__all__ = ['ChinaStockKdataRecorder']

if __name__ == '__main__':
    recorder = ChinaStockKdataRecorder(level=IntervalLevel.LEVEL_1MON)
    recorder.run()
