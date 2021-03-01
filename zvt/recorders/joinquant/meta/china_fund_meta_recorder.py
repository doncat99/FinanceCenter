# -*- coding: utf-8 -*-
from datetime import datetime

import pandas as pd
import numpy as np

from zvt import zvt_config
from zvt.api.data_type import Region, Provider
from zvt.api.quote import china_stock_code_to_id
from zvt.domain.meta.fund_meta import Fund, FundStock
from zvt.contract.recorder import Recorder, TimeSeriesDataRecorder
from zvt.contract.api import df_to_db
from zvt.recorders.joinquant.common import to_entity_id, jq_to_report_period
from zvt.networking.request import jq_run_query
from zvt.utils.pd_utils import pd_is_not_null
from zvt.utils.time_utils import to_time_str, next_date, now_pd_timestamp, is_same_date, PD_TIME_FORMAT_DAY


class JqChinaFundRecorder(Recorder):
    region = Region.CHN
    provider = Provider.JoinQuant
    data_schema = Fund

    def run(self):
        # 按不同类别抓取
        # 编码    基金运作方式
        # 401001    开放式基金
        # 401002    封闭式基金
        # 401003    QDII
        # 401004    FOF
        # 401005    ETF
        # 401006    LOF
        for operate_mode_id in (401001, 401002, 401005):
            year_count = 2
            while True:
                latest = Fund.query_data(region=self.region, filters=[Fund.operate_mode_id == operate_mode_id], order=Fund.timestamp.desc(),
                                         limit=1, return_type='domain')
                start_timestamp = '2000-01-01'
                if latest:
                    start_timestamp = latest[0].timestamp

                end_timestamp = min(next_date(start_timestamp, 365 * year_count), now_pd_timestamp(self.region))

                df = jq_run_query(table='finance.FUND_MAIN_INFO',
                                  conditions=f'operate_mode_id#=#{operate_mode_id}&start_date#>=#{to_time_str(start_timestamp)}&start_date#<=#{to_time_str(end_timestamp)}',
                                  parse_dates=['start_date', 'end_date'],
                                  dtype={'main_code': str})
                if not pd_is_not_null(df) or (df['start_date'].max().year < end_timestamp.year):
                    year_count = year_count + 1

                if pd_is_not_null(df):
                    df.rename(columns={'start_date': 'timestamp'}, inplace=True)
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df['list_date'] = df['timestamp']
                    df['end_date'] = pd.to_datetime(df['end_date'])

                    df['code'] = df['main_code']
                    df['entity_id'] = df['code'].apply(lambda x: to_entity_id(entity_type='fund', jq_code=x))
                    df['id'] = df['entity_id']
                    df['entity_type'] = 'fund'
                    df['exchange'] = 'sz'
                    df_to_db(df, ref_df=None, region=self.region, data_schema=Fund, provider=self.provider)
                    self.logger.info(
                        f'persist fund {operate_mode_id} list success {start_timestamp} to {end_timestamp}')

                if is_same_date(end_timestamp, now_pd_timestamp(self.region)):
                    break


class JqChinaFundStockRecorder(TimeSeriesDataRecorder):
    region = Region.CHN
    provider = Provider.JoinQuant
    entity_schema = Fund
    data_schema = FundStock

    def __init__(self, entity_ids=None, codes=None, batch_size=10, force_update=False,
                 sleeping_time=5, default_size=zvt_config['batch_size'], real_time=False,
                 fix_duplicate_way='add', start_timestamp=None, end_timestamp=None,
                 close_hour=0, close_minute=0) -> None:
        super().__init__('fund', ['sh', 'sz'], entity_ids, codes, batch_size, force_update, sleeping_time,
                         default_size, real_time, fix_duplicate_way, start_timestamp, end_timestamp, close_hour,
                         close_minute)

    def init_entities(self):
        # 只抓股票型，混合型并且没退市的持仓,
        self.entities = Fund.query_data(
            region=self.region,
            entity_ids=self.entity_ids,
            codes=self.codes,
            return_type='domain',
            provider=self.provider,
            filters=[Fund.underlying_asset_type.in_(('股票型', '混合型')), Fund.end_date.is_(None)])

    def get_original_time_field(self):
        return 'pub_date'

    def generate_domain_id(self, entity, df, time_fmt=PD_TIME_FORMAT_DAY):
        return df[['entity_id', 'stock_id', 'pub_date', 'id']].apply(lambda x: '_'.join(x.astype(str)), axis=1)

    def record(self, entity, start, end, size, timestamps, http_session):
        # 忽略退市的
        if entity.end_date:
            return None

        df = jq_run_query(table='finance.FUND_PORTFOLIO_STOCK',
                          conditions=f'pub_date#>=#{to_time_str(start)}&code#=#{entity.code}',
                          parse_dates=None)

        df = df.dropna()

        if pd_is_not_null(df):
            return df
        return None

    def format(self, entity, df):
        #          id    code period_start  period_end    pub_date  report_type_id report_type  rank  symbol  name      shares    market_cap  proportion
        # 0   8640569  159919   2018-07-01  2018-09-30  2018-10-26          403003        第三季度     1  601318  中国平安  19869239.0  1.361043e+09        7.09
        # 1   8640570  159919   2018-07-01  2018-09-30  2018-10-26          403003        第三季度     2  600519  贵州茅台    921670.0  6.728191e+08        3.50
        # 2   8640571  159919   2018-07-01  2018-09-30  2018-10-26          403003        第三季度     3  600036  招商银行  18918815.0  5.806184e+08        3.02
        # 3   8640572  159919   2018-07-01  2018-09-30  2018-10-26          403003        第三季度     4  601166  兴业银行  22862332.0  3.646542e+08        1.90

        df.rename(columns={'symbol': 'stock_code', 'name': 'stock_name'}, inplace=True)

        df.update(df.select_dtypes(include=[np.number]).fillna(0))

        df['proportion'] *= 0.01
        df['stock_id'] = df['stock_code'].apply(lambda x: china_stock_code_to_id(x))
        df['report_date'] = pd.to_datetime(df['period_end'])
        df['report_period'] = df['report_type'].apply(lambda x: jq_to_report_period(x))

        if 'timestamp' not in df.columns:
            df['timestamp'] = pd.to_datetime(df[self.get_original_time_field()])
        elif not isinstance(df['timestamp'].dtypes, datetime):
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        df['entity_id'] = entity.id
        df['provider'] = self.provider.value
        df['entity_type'] = entity.entity_type
        df['exchange'] = entity.exchange
        df['code'] = entity.code
        df['name'] = entity.name

        df['id'] = self.generate_domain_id(entity, df)
        return df


if __name__ == '__main__':
    # JqChinaFundRecorder().run()
    JqChinaFundStockRecorder(codes=['000053']).run()


# the __all__ is generated
__all__ = ['JqChinaFundRecorder', 'JqChinaFundStockRecorder']
