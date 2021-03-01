# -*- coding: utf-8 -*-
import argparse

import pandas as pd

from zvt import init_log, zvt_config
from zvt.api.data_type import Region, Provider, EntityType
from zvt.api.quote import get_kdata, get_kdata_schema
from zvt.domain import Stock, StockKdataCommon, Stock1dHfqKdata
from zvt.contract import IntervalLevel, AdjustType
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.recorders.baostock.common import to_bao_trading_level, to_bao_entity_id, \
                                          to_bao_trading_field, to_bao_adjust_flag
from zvt.networking.request import bao_get_bars
from zvt.utils.pd_utils import pd_is_not_null
from zvt.utils.time_utils import to_time_str, PD_TIME_FORMAT_DAY, PD_TIME_FORMAT_ISO8601


class BaoChinaStockKdataRecorder(FixedCycleDataRecorder):
    # 数据来自jq
    region = Region.CHN
    provider = Provider.BaoStock
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
        self.bao_trading_level = to_bao_trading_level(level)

        super().__init__(EntityType.Stock, exchanges, entity_ids, codes, batch_size, force_update, sleeping_time,
                         default_size, real_time, fix_duplicate_way, start_timestamp, end_timestamp, close_hour,
                         close_minute, level, kdata_use_begin_time, one_day_trading_minutes, share_para=share_para)
        self.adjust_type = adjust_type

        def on_finish(self):
            super().on_finish()

    def generate_domain_id(self, entity, df, time_fmt=PD_TIME_FORMAT_DAY):
        format = PD_TIME_FORMAT_DAY if self.level >= IntervalLevel.LEVEL_1DAY else PD_TIME_FORMAT_ISO8601
        return df['entity_id'] + '_' + df[self.get_evaluated_time_field()].dt.strftime(format)

    def record(self, entity, start, end, size, timestamps, http_session):
        start = to_time_str(start)

        if self.bao_trading_level in ['d', 'w', 'm']:
            start = start if start > "1990-12-19" else "1990-12-19"
        else:
            start = start if start > "1999-07-26" else "1999-07-26"

        if end is None:
            df = bao_get_bars(to_bao_entity_id(entity),
                              start=start,
                              end=end,
                              frequency=self.bao_trading_level,
                              fields=to_bao_trading_field(self.bao_trading_level),
                              adjustflag=to_bao_adjust_flag(self.adjust_type))
        else:
            df = bao_get_bars(to_bao_entity_id(entity),
                              start=start,
                              end=to_time_str(end),
                              frequency=self.bao_trading_level,
                              fields=to_bao_trading_field(self.bao_trading_level),
                              adjustflag=to_bao_adjust_flag(self.adjust_type))

        if pd_is_not_null(df):
            return df
        return None

    def format(self, entity, df):
        if self.bao_trading_level == 'd':
            df.rename(columns={'turn': 'turnover', 'date': 'timestamp', 'preclose': 'pre_close', 'pctChg': 'change_pct',
                               'peTTM': 'pe_ttm', 'psTTM': 'ps_ttm', 'pcfNcfTTM': 'pcf_ncf_ttm', 'pbMRQ': 'pb_mrq', 'isST': 'is_st'}, inplace=True)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['is_st'] = df['is_st'].astype(int)

        elif self.bao_trading_level == 'w' or self.bao_trading_level == 'm':
            df.rename(columns={'turn': 'turnover', 'date': 'timestamp', 'pctChg': 'change_pct'}, inplace=True)
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        else:
            df.rename(columns={'time': 'timestamp'}, inplace=True)
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y%m%d%H%M%S%f')

        cols = df.select_dtypes('object').columns.to_list()
        cols.remove('adjustflag')
        df.replace(r'^\s*$', 0.0, regex=True, inplace=True)
        df[cols] = df[cols].astype(float)

        df['entity_id'] = entity.id
        df['provider'] = self.provider.value
        df['name'] = entity.name
        df['code'] = entity.code
        df['level'] = self.level.value
        df.replace({'adjustflag': {'1': 'hfq', '2': 'qfq', '3': 'normal'}}, inplace=True)

        df['id'] = self.generate_domain_id(entity, df)
        return df


__all__ = ['BaoChinaStockKdataRecorder']

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--level', help='trading level', default='1d', choices=[item.value for item in IntervalLevel])
    parser.add_argument('--codes', help='codes', default=['000001'], nargs='+')

    args = parser.parse_args()

    level = IntervalLevel(args.level)
    codes = args.codes

    init_log('bao_china_stock_{}_kdata.log'.format(args.level))
    BaoChinaStockKdataRecorder(level=level, sleeping_time=0, codes=codes,
                               real_time=False, adjust_type=AdjustType.hfq).run()

    print(get_kdata(region=Region.CHN, entity_id='stock_sz_000001', limit=10, order=Stock1dHfqKdata.timestamp.desc(),
                    adjust_type=AdjustType.hfq))
