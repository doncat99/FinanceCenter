# -*- coding: utf-8 -*-
from datetime import datetime

import pandas as pd
# from numba import njit

from zvt import zvt_config
from zvt.api.data_type import Region, Provider, EntityType
from zvt.domain import BlockMoneyFlow, BlockCategory, Block
from zvt.contract import IntervalLevel
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.networking.request import sync_get
from zvt.utils.time_utils import to_pd_timestamp
from zvt.utils.utils import to_float


# 实时资金流
# 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_bkzj_bk?page=1&num=20&sort=netamount&asc=0&fenlei=1'
# 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_bkzj_bk?page=1&num=20&sort=netamount&asc=0&fenlei=0'


class SinaBlockMoneyFlowRecorder(FixedCycleDataRecorder):
    # 记录的信息从哪里来
    region = Region.CHN
    provider = Provider.Sina
    # entity的schema
    entity_schema = Block
    # 记录的schema
    data_schema = BlockMoneyFlow

    url = 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_bkzj_zjlrqs?page=1&num={}&sort=opendate&asc=0&bankuai={}%2F{}'

    def __init__(self, exchanges=None, entity_ids=None, codes=None, batch_size=10,
                 force_update=True, sleeping_time=10, default_size=zvt_config['batch_size'],
                 real_time=False, fix_duplicate_way='ignore', start_timestamp=None,
                 end_timestamp=None, close_hour=0, close_minute=0,
                 level=IntervalLevel.LEVEL_1DAY, kdata_use_begin_time=False,
                 one_day_trading_minutes=24 * 60) -> None:
        super().__init__(EntityType.Block, exchanges, entity_ids, codes, batch_size,
                         force_update, sleeping_time, default_size, real_time,
                         fix_duplicate_way, start_timestamp, end_timestamp, close_hour,
                         close_minute, level, kdata_use_begin_time, one_day_trading_minutes)

    def generate_url(self, category, code, number):
        if category == BlockCategory.industry.value:
            block = 0
        elif category == BlockCategory.concept.value:
            block = 1

        return self.url.format(number, block, code)

    def record(self, entity, start, end, size, timestamps, http_session):
        url = self.generate_url(category=entity.category, code=entity.code, number=size)
        text = sync_get(http_session, url, return_type='text')
        if text is None:
            return None

        json_list = eval(text)
        if len(json_list) == 0:
            return None

        # @njit(nopython=True)
        def numba_boost_up(json_list):
            result_list = []
            for item in json_list:
                result_list.append({
                    'name': entity.name,
                    'timestamp': to_pd_timestamp(item['opendate']),
                    'close': to_float(item['avg_price']),
                    'change_pct': to_float(item['avg_changeratio']),
                    'turnover_rate': to_float(item['turnover']) / 10000,
                    'net_inflows': to_float(item['netamount']),
                    'net_inflow_rate': to_float(item['ratioamount']),
                    'net_main_inflows': to_float(item['r0_net']),
                    'net_main_inflow_rate': to_float(item['r0_ratio'])
                })

            return result_list

        result_list = numba_boost_up(json_list)

        if len(result_list) > 0:
            df = pd.DataFrame.from_records(result_list)
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

        df['id'] = self.generate_domain_id(entity, df)
        return df


__all__ = ['SinaBlockMoneyFlowRecorder']


if __name__ == '__main__':
    SinaBlockMoneyFlowRecorder(codes=['new_fjzz']).run()
    # SinaIndexMoneyFlowRecorder().run()
