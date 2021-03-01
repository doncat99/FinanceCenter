# -*- coding: utf-8 -*-
import argparse
from datetime import datetime

import pandas as pd

from zvt import init_log, zvt_config
from zvt.api.data_type import Region, Provider, EntityType
from zvt.api.quote import get_kdata_schema, get_kdata
from zvt.domain import Stock, StockKdataCommon, Stock1dHfqKdata
from zvt.contract import IntervalLevel, AdjustType
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.contract.api import get_db_session
from zvt.recorders.joinquant.common import to_jq_trading_level, to_jq_entity_id
from zvt.networking.request import jq_get_token, jq_get_bars
from zvt.utils.pd_utils import pd_is_not_null
from zvt.utils.time_utils import to_time_str, now_pd_timestamp, PD_TIME_FORMAT_DAY, PD_TIME_FORMAT_ISO8601


class JqChinaStockKdataRecorder(FixedCycleDataRecorder):
    # 数据来自jq
    region = Region.CHN
    provider = Provider.JoinQuant
    entity_schema = Stock
    # 只是为了把recorder注册到data_schema
    data_schema = StockKdataCommon

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
                 one_day_trading_minutes=4 * 60,
                 adjust_type=AdjustType.qfq,
                 share_para=None) -> None:
        level = IntervalLevel(level)
        adjust_type = AdjustType(adjust_type)
        self.data_schema = get_kdata_schema(entity_type=EntityType.Stock, level=level, adjust_type=adjust_type)
        self.jq_trading_level = to_jq_trading_level(level)

        super().__init__(EntityType.Stock, exchanges, entity_ids, codes, batch_size, force_update, sleeping_time,
                         default_size, real_time, fix_duplicate_way, start_timestamp, end_timestamp, close_hour,
                         close_minute, level, kdata_use_begin_time, one_day_trading_minutes, share_para=share_para)
        self.adjust_type = adjust_type

        jq_get_token(zvt_config['jq_username'], zvt_config['jq_password'], force=True)

    def recompute_qfq(self, entity, qfq_factor, last_timestamp):
        # 重新计算前复权数据
        if qfq_factor != 0:
            kdatas = get_kdata(region=self.region,
                               provider=self.provider,
                               entity_id=entity.id,
                               level=self.level.value,
                               order=self.data_schema.timestamp.asc(),
                               return_type='domain',
                               filters=[self.data_schema.timestamp < last_timestamp])
            if kdatas:
                self.logger.info('recomputing {} qfq kdata,factor is:{}'.format(entity.code, qfq_factor))
                for kdata in kdatas:
                    kdata.open = round(kdata.open * qfq_factor, 2)
                    kdata.close = round(kdata.close * qfq_factor, 2)
                    kdata.high = round(kdata.high * qfq_factor, 2)
                    kdata.low = round(kdata.low * qfq_factor, 2)
                session = get_db_session(region=self.region,
                                         provider=self.provider,
                                         data_schema=self.data_schema)
                session.bulk_save_objects(kdatas)
                session.commit()

    def generate_domain_id(self, entity, df, time_fmt=PD_TIME_FORMAT_DAY):
        format = PD_TIME_FORMAT_DAY if self.level >= IntervalLevel.LEVEL_1DAY else PD_TIME_FORMAT_ISO8601
        return entity.id + '_' + df[self.get_evaluated_time_field()].dt.strftime(format)

    def record(self, entity, start, end, size, timestamps, http_session):
        if self.adjust_type == AdjustType.hfq:
            fq_ref_date = '2000-01-01'
        else:
            fq_ref_date = to_time_str(now_pd_timestamp(Region.CHN))

        if not self.end_timestamp:
            df = jq_get_bars(to_jq_entity_id(entity),
                             count=size,
                             unit=self.jq_trading_level,
                             # fields=['date', 'open', 'close', 'low', 'high', 'volume', 'money'],
                             fq_ref_date=fq_ref_date)
        else:
            end_timestamp = to_time_str(self.end_timestamp)
            df = jq_get_bars(to_jq_entity_id(entity),
                             count=size,
                             unit=self.jq_trading_level,
                             # fields=['date', 'open', 'close', 'low', 'high', 'volume', 'money'],
                             end_date=end_timestamp,
                             fq_ref_date=fq_ref_date)
        # self.logger.info("record {} for {}, size:{}".format(self.data_schema.__name__, entity.id, len(df)))

        if pd_is_not_null(df):
            # start_timestamp = to_time_str(df.iloc[1]['timestamp'])
            # end_timestamp = to_time_str(df.iloc[-1]['timestamp'])

            # 判断是否需要重新计算之前保存的前复权数据
            if self.adjust_type == AdjustType.qfq:
                check_df = df.head(1)
                check_date = check_df['timestamp'][0]
                current_df = get_kdata(region=self.region,
                                       entity_id=entity.id,
                                       provider=self.provider,
                                       start_timestamp=check_date,
                                       end_timestamp=check_date,
                                       limit=1,
                                       level=self.level,
                                       adjust_type=self.adjust_type)
                if pd_is_not_null(current_df):
                    old = current_df.iloc[0, :]['close']
                    new = check_df['close'][0]
                    # 相同时间的close不同，表明前复权需要重新计算
                    if round(old, 2) != round(new, 2):
                        qfq_factor = new / old
                        last_timestamp = pd.Timestamp(check_date)
                        self.recompute_qfq(entity, qfq_factor=qfq_factor, last_timestamp=last_timestamp)
            return df

        return None

    def format(self, entity, df):
        df.rename(columns={'money': 'turnover', 'date': 'timestamp'}, inplace=True)

        if 'timestamp' not in df.columns:
            df['timestamp'] = pd.to_datetime(df[self.get_original_time_field()])
        elif not isinstance(df['timestamp'].dtypes, datetime):
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        # df.update(df.select_dtypes(include=[np.number]).fillna(0))

        df['entity_id'] = entity.id
        df['provider'] = self.provider.value
        df['code'] = entity.code
        df['name'] = entity.name
        df['level'] = self.level.value

        df['id'] = self.generate_domain_id(entity, df)
        return df


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--level', help='trading level', default='1d', choices=[item.value for item in IntervalLevel])
    parser.add_argument('--codes', help='codes', default=['000001'], nargs='+')

    args = parser.parse_args()

    level = IntervalLevel(args.level)
    codes = args.codes

    init_log('jq_china_stock_{}_kdata.log'.format(args.level))
    JqChinaStockKdataRecorder(level=level, sleeping_time=0, codes=codes, real_time=False,
                              adjust_type=AdjustType.hfq).run()

    print(get_kdata(region=Region.CHN, entity_id='stock_sz_000001', limit=10, order=Stock1dHfqKdata.timestamp.desc(),
                    adjust_type=AdjustType.hfq))


# the __all__ is generated
__all__ = ['JqChinaStockKdataRecorder']
