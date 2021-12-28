# -*- coding: utf-8 -*-
from datetime import datetime
import time

import pandas as pd
# from numba import njit

from findy.interface import Region, Provider, EntityType
from findy.database.schema import IntervalLevel, BlockCategory
from findy.database.schema.meta.stock_meta import Index, Block
from findy.database.plugins.recorder import KDataRecorder
from findy.database.quote import get_entities, get_kdata_schema
from findy.utils.time import now_time_str, TIME_FORMAT_DAY1, PD_TIME_FORMAT_DAY, PD_TIME_FORMAT_ISO8601
from findy.utils.convert import json_callback_param, to_float


def level_flag(level: IntervalLevel):
    level = IntervalLevel(level)
    if level == IntervalLevel.LEVEL_1DAY:
        return 101
    if level == IntervalLevel.LEVEL_1WEEK:
        return 102
    if level == IntervalLevel.LEVEL_1MON:
        return 103


# 抓取行业的日线,周线,月线数据，用于中期选行业
class ChinaStockKdataRecorder(KDataRecorder):
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

    async def init_entities(self, db_session):
        self.entities, column_names = get_entities(
            region=self.region,
            provider=self.provider,
            db_session=db_session,
            entity_type=EntityType.Index,
            exchanges=self.exchanges,
            codes=self.codes,
            entity_ids=self.entity_ids,
            # 只抓概念和行业
            filters=[Index.category.in_([BlockCategory.industry.value, BlockCategory.concept.value])])

    def generate_domain_id(self, entity, df, time_fmt=PD_TIME_FORMAT_DAY):
        format = PD_TIME_FORMAT_DAY if self.level >= IntervalLevel.LEVEL_1DAY else PD_TIME_FORMAT_ISO8601
        return entity.id + '_' + df[self.get_evaluated_time_field()].dt.strftime(format)

    async def record(self, entity, http_session, db_session, para):
        start_point = time.time()

        (ref_record, start, end, size, timestamps) = para

        url = self.url.format("{}".format(entity.code), level_flag(self.level), size,
                              now_time_str(region=Region.CHN, fmt=TIME_FORMAT_DAY1))
        async with http_session.get(url) as response:
            text = await response.text()
            if text is None:
                return True, time.time() - start_point, None

            results = json_callback_param(text)

            if results:
                klines = results['data']['klines']

                # @njit(nopython=True)
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
                    return False, time.time() - start_point, (ref_record, self.format(entity, df))
            return True, time.time() - start_point, None

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

    async def on_finish_entity(self, entity, http_session, db_session, result):
        return 0

    async def on_finish(self):
        pass
