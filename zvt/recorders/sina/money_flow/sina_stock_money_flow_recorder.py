# -*- coding: utf-8 -*-
from datetime import datetime

import pandas as pd
# from numba import njit

from zvt import zvt_config
from zvt.api.data_type import Region, Provider, EntityType
from zvt.domain import StockMoneyFlow, Stock
from zvt.contract import IntervalLevel
from zvt.contract.recorder import TimeSeriesDataRecorder
from zvt.networking.request import sync_get
from zvt.utils.time_utils import to_pd_timestamp, now_pd_timestamp
from zvt.utils.utils import to_float


class SinaStockMoneyFlowRecorder(TimeSeriesDataRecorder):
    region = Region.CHN
    provider = Provider.Sina
    entity_schema = Stock
    data_schema = StockMoneyFlow

    url = 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_lscjfb?page=1&num={}&sort=opendate&asc=0&daima={}'

    def __init__(self, exchanges=None, entity_ids=None, codes=None, batch_size=10,
                 force_update=True, sleeping_time=10, default_size=zvt_config['batch_size'],
                 real_time=False, fix_duplicate_way='ignore', start_timestamp=None,
                 end_timestamp=None, close_hour=0, close_minute=0,
                 level=IntervalLevel.LEVEL_1DAY, kdata_use_begin_time=False,
                 one_day_trading_minutes=24 * 60, share_para=None) -> None:
        super().__init__(EntityType.Stock, exchanges, entity_ids, codes, batch_size,
                         force_update, sleeping_time, default_size, real_time,
                         fix_duplicate_way, start_timestamp, end_timestamp, close_hour,
                         close_minute, share_para=share_para)

    def init_entities(self):
        super().init_entities()
        # 过滤掉退市的
        self.entities = [entity for entity in self.entities if
                         (entity.end_date is None) or (entity.end_date > now_pd_timestamp(Region.CHN))]

    def generate_url(self, code, number):
        return self.url.format(number, code)

    def record(self, entity, start, end, size, timestamps, http_session):
        param = {
            'url': self.generate_url(code='{}{}'.format(entity.exchange, entity.code), number=size),
            'security_item': entity
        }
        url = param['url']
        text = sync_get(http_session, url, return_type='text')
        if text is None:
            return None

        json_list = eval(text)
        if len(json_list) == 0:
            return None

        # @njit(nopython=True)
        def numba_boost_up(json_list):
            result_list = []
            # {opendate:"2019-04-29",trade:"10.8700",changeratio:"-0.0431338",turnover:"74.924",netamount:"-2903349.8500",
            # ratioamount:"-0.155177",r0:"0.0000",r1:"2064153.0000",r2:"6485031.0000",r3:"10622169.2100",r0_net:"0.0000",
            # r1_net:"2064153.0000",r2_net:"-1463770.0000",r3_net:"-3503732.8500"}
            for item in json_list:
                result = {
                    'timestamp': to_pd_timestamp(item['opendate']),
                    'close': to_float(item['trade']),
                    'change_pct': to_float(item['changeratio']),
                    'turnover_rate': to_float(item['turnover']) / 10000,
                    'net_inflows': to_float(item['netamount']),
                    'net_inflow_rate': to_float(item['ratioamount']),
                    #     # 主力=超大单+大单
                    #     net_main_inflows = Column(Float)
                    #     net_main_inflow_rate = Column(Float)
                    #     # 超大单
                    #     net_huge_inflows = Column(Float)
                    #     net_huge_inflow_rate = Column(Float)
                    #     # 大单
                    #     net_big_inflows = Column(Float)
                    #     net_big_inflow_rate = Column(Float)
                    #
                    #     # 中单
                    #     net_medium_inflows = Column(Float)
                    #     net_medium_inflow_rate = Column(Float)
                    #     # 小单
                    #     net_small_inflows = Column(Float)
                    #     net_small_inflow_rate = Column(Float)
                    'net_main_inflows': to_float(item['r0_net']) + to_float(item['r1_net']),
                    'net_huge_inflows': to_float(item['r0_net']),
                    'net_big_inflows': to_float(item['r1_net']),
                    'net_medium_inflows': to_float(item['r2_net']),
                    'net_small_inflows': to_float(item['r3_net']),
                }

                amount = to_float(item['r0']) + to_float(item['r1']) + to_float(item['r2']) + to_float(item['r3'])
                if amount != 0:
                    result['net_main_inflow_rate'] = (to_float(item['r0_net']) + to_float(item['r1_net'])) / amount
                    result['net_huge_inflow_rate'] = to_float(item['r0_net']) / amount
                    result['net_big_inflow_rate'] = to_float(item['r1_net']) / amount
                    result['net_medium_inflow_rate'] = to_float(item['r2_net']) / amount
                    result['net_small_inflow_rate'] = to_float(item['r3_net']) / amount

                result_list.append(result)

            return result_list

        result_list = numba_boost_up(json_list)
        df = pd.DataFrame.from_records(result_list)
        return df

    def format(self, entity, df):
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


__all__ = ['SinaStockMoneyFlowRecorder']

if __name__ == '__main__':
    SinaStockMoneyFlowRecorder(codes=['000406']).run()
    # SinaStockMoneyFlowRecorder().run()
