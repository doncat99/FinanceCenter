# -*- coding: utf-8 -*-
from datetime import datetime

import pandas as pd

from zvt import zvt_config
from zvt.api.data_type import Region, Provider, EntityType
from zvt.domain import StockMoneyFlow, Stock
from zvt.contract import IntervalLevel
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.recorders.joinquant.common import to_jq_entity_id
from zvt.recorders.joinquant.misc.joinquant_index_money_flow_recorder import JoinquantIndexMoneyFlowRecorder
from zvt.networking.request import jq_get_token, jq_get_money_flow
from zvt.utils.pd_utils import pd_is_not_null
from zvt.utils.time_utils import to_time_str


class JoinquantStockMoneyFlowRecorder(FixedCycleDataRecorder):
    region = Region.CHN
    provider = Provider.JoinQuant
    entity_schema = Stock
    data_schema = StockMoneyFlow

    def __init__(self, entity_type=EntityType.Stock, exchanges=['sh', 'sz'],
                 entity_ids=None, codes=None, batch_size=10, force_update=True,
                 sleeping_time=0, default_size=zvt_config['batch_size'], real_time=False,
                 fix_duplicate_way='ignore', start_timestamp=None, end_timestamp=None,
                 close_hour=0, close_minute=0, level=IntervalLevel.LEVEL_1DAY,
                 kdata_use_begin_time=False, one_day_trading_minutes=24 * 60,
                 compute_index_money_flow=False) -> None:
        self.compute_index_money_flow = compute_index_money_flow
        super().__init__(entity_type, exchanges, entity_ids, codes, batch_size,
                         force_update, sleeping_time, default_size, real_time,
                         fix_duplicate_way, start_timestamp, end_timestamp, close_hour,
                         close_minute, level, kdata_use_begin_time,
                         one_day_trading_minutes)
        jq_get_token(zvt_config['jq_username'], zvt_config['jq_password'], force=True)

    def on_finish(self):
        # 根据 个股资金流 计算 大盘资金流
        if self.compute_index_money_flow:
            JoinquantIndexMoneyFlowRecorder().run()

    def record(self, entity, start, end, size, timestamps, http_session):
        if not self.end_timestamp:
            df = jq_get_money_flow(code=to_jq_entity_id(entity),
                                   date=to_time_str(start))
        else:
            df = jq_get_money_flow(code=to_jq_entity_id(entity),
                                   date=start, end_date=to_time_str(self.end_timestamp))

        df = df.dropna()

        if pd_is_not_null(df):
            return df
        return None

    def format(self, entity, df):
        df.rename(columns={'date': 'timestamp',
                           'net_amount_main': 'net_main_inflows',
                           'net_pct_main': 'net_main_inflow_rate',
                           'net_amount_xl': 'net_huge_inflows',
                           'net_pct_xl': 'net_huge_inflow_rate',
                           'net_amount_l': 'net_big_inflows',
                           'net_pct_l': 'net_big_inflow_rate',
                           'net_amount_m': 'net_medium_inflows',
                           'net_pct_m': 'net_medium_inflow_rate',
                           'net_amount_s': 'net_small_inflows',
                           'net_pct_s': 'net_small_inflow_rate'}, inplace=True)

        # 转换到标准float
        inflows_cols = ['net_main_inflows', 'net_huge_inflows', 'net_big_inflows', 'net_medium_inflows',
                        'net_small_inflows']
        for col in inflows_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna()

        if not pd_is_not_null(df):
            return None

        df[inflows_cols] *= 10000

        inflow_rate_cols = ['net_main_inflow_rate', 'net_huge_inflow_rate', 'net_big_inflow_rate',
                            'net_medium_inflow_rate', 'net_small_inflow_rate']

        for col in inflow_rate_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna()

        if not pd_is_not_null(df):
            return None

        df[inflow_rate_cols] /= 100

        # 计算总流入
        df['net_inflows'] = df['net_huge_inflows'] + df['net_big_inflows'] + df['net_medium_inflows'] + df['net_small_inflows']
        # 计算总流入率
        amount = df['net_main_inflows'] / df['net_main_inflow_rate']
        df['net_inflow_rate'] = df['net_inflows'] / amount

        if 'timestamp' not in df.columns:
            df['timestamp'] = pd.to_datetime(df[self.get_original_time_field()])
        elif not isinstance(df['timestamp'].dtypes, datetime):
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        df['entity_id'] = entity.id
        df['provider'] = self.provider.value
        df['code'] = entity.code
        df['name'] = entity.name
        df['level'] = self.level.value

        df['id'] = self.generate_domain_id(entity, df)
        return df


if __name__ == '__main__':
    JoinquantStockMoneyFlowRecorder(codes=['000578']).run()


# the __all__ is generated
__all__ = ['JoinquantStockMoneyFlowRecorder']
