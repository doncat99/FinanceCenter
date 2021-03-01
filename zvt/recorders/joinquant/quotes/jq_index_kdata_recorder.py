# -*- coding: utf-8 -*-
from datetime import datetime

import pandas as pd

from zvt import zvt_config
from zvt.api.data_type import Region, Provider, EntityType
from zvt.api.quote import get_kdata_schema, get_kdata
from zvt.domain import Index, IndexKdataCommon
from zvt.contract import IntervalLevel
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.recorders.joinquant.common import to_jq_trading_level, to_jq_entity_id
from zvt.networking.request import jq_get_token, jq_get_bars
from zvt.utils.pd_utils import pd_is_not_null
from zvt.utils.time_utils import to_time_str, PD_TIME_FORMAT_DAY, PD_TIME_FORMAT_ISO8601


class JqChinaIndexKdataRecorder(FixedCycleDataRecorder):
    # 数据来自jq
    region = Region.CHN
    provider = Provider.JoinQuant
    entity_schema = Index
    # 只是为了把recorder注册到data_schema
    data_schema = IndexKdataCommon

    def __init__(self,
                 exchanges=['sh', 'sz'],
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
                 one_day_trading_minutes=4 * 60) -> None:
        level = IntervalLevel(level)
        self.data_schema = get_kdata_schema(entity_type=EntityType.Index, level=level)
        self.jq_trading_level = to_jq_trading_level(level)

        super().__init__(EntityType.Index, exchanges, entity_ids, codes, batch_size, force_update, sleeping_time,
                         default_size, real_time, fix_duplicate_way, start_timestamp, end_timestamp, close_hour,
                         close_minute, level, kdata_use_begin_time, one_day_trading_minutes)

        jq_get_token(zvt_config['jq_username'], zvt_config['jq_password'], force=True)

    def init_entities(self):
        super().init_entities()
        # ignore no data index
        self.entities = [entity for entity in self.entities if
                         entity.code not in ['310001', '310002', '310003', '310004']]

    def generate_domain_id(self, entity, df, time_fmt=PD_TIME_FORMAT_DAY):
        format = PD_TIME_FORMAT_DAY if self.level >= IntervalLevel.LEVEL_1DAY else PD_TIME_FORMAT_ISO8601
        return entity.id + '_' + df[self.get_evaluated_time_field()].dt.strftime(format)

    def record(self, entity, start, end, size, timestamps, http_session):
        if not self.end_timestamp:
            df = jq_get_bars(to_jq_entity_id(entity),
                             count=size,
                             unit=self.jq_trading_level,
                             # fields=['date', 'open', 'close', 'low', 'high', 'volume', 'money']
                             )
        else:
            end_timestamp = to_time_str(self.end_timestamp)
            df = jq_get_bars(to_jq_entity_id(entity),
                             count=size,
                             unit=self.jq_trading_level,
                             # fields=['date', 'open', 'close', 'low', 'high', 'volume', 'money'],
                             end_date=end_timestamp)

        if pd_is_not_null(df):
            return df
        return None

    def format(self, entity, df):
        if 'timestamp' not in df.columns:
            df['timestamp'] = pd.to_datetime(df[self.get_original_time_field()])
        elif not isinstance(df['timestamp'].dtypes, datetime):
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        df.rename(columns={'money': 'turnover', 'date': 'timestamp'}, inplace=True)

        # df.update(df.select_dtypes(include=[np.number]).fillna(0))

        df['entity_id'] = entity.id
        df['provider'] = self.provider.value
        df['code'] = entity.code
        df['name'] = entity.name
        df['level'] = self.level.value

        df['id'] = self.generate_domain_id(entity, df)
        return df


if __name__ == '__main__':
    from zvt import init_log
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--level', help='trading level', default='1d', choices=[item.value for item in IntervalLevel])
    parser.add_argument('--codes', help='codes', default=['000001'], nargs='+')

    args = parser.parse_args()

    level = IntervalLevel(args.level)
    codes = args.codes

    init_log('jq_china_stock_{}_kdata.log'.format(args.level))
    JqChinaIndexKdataRecorder(level=level, sleeping_time=0, codes=codes, real_time=False).run()

    print(get_kdata(region=Region.CHN, entity_id='index_sz_000001', limit=10))


# the __all__ is generated
__all__ = ['JqChinaIndexKdataRecorder']
