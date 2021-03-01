from datetime import datetime

import pandas as pd
import numpy as np

from zvt import zvt_config
from zvt.api.data_type import Region, Provider, EntityType
from zvt.domain import Index, MarginTradingSummary
from zvt.contract.recorder import TimeSeriesDataRecorder
from zvt.networking.request import jq_run_query
from zvt.utils.time_utils import to_time_str
from zvt.utils.pd_utils import pd_is_not_null


# 聚宽编码
# XSHG-上海证券交易所
# XSHE-深圳证券交易所

code_map_jq = {
    '000001': 'XSHG',
    '399106': 'XSHE'
}


class MarginTradingSummaryRecorder(TimeSeriesDataRecorder):
    region = Region.CHN
    provider = Provider.JoinQuant
    entity_schema = Index
    data_schema = MarginTradingSummary

    def __init__(self, batch_size=10,
                 force_update=False, sleeping_time=5, default_size=zvt_config['batch_size'],
                 real_time=False, fix_duplicate_way='add', share_para=None) -> None:
        # 上海A股,深圳市场
        codes = ['000001', '399106']
        super().__init__(EntityType.Index, ['sh', 'sz'], None, codes, batch_size,
                         force_update, sleeping_time,
                         default_size, real_time, fix_duplicate_way, share_para=share_para)

    def record(self, entity, start, end, size, timestamps, http_session):
        jq_code = code_map_jq.get(entity.code)

        df = jq_run_query(table='finance.STK_MT_TOTAL',
                          conditions=f'exchange_code#=#{jq_code}&date#>=#{to_time_str(start)}', parse_dates=['date'])

        if pd_is_not_null(df):
            if len(df) < 100:
                self.one_shot = True
            return df
        return None

    def format(self, entity, df):
        cols = ['date', 'fin_value', 'fin_buy_value', 'sec_value', 'sec_sell_volume', 'fin_sec_value']
        df = df[cols].copy()
        df.rename(columns={'date': 'timestamp', 'fin_value': 'margin_value',
                           'fin_buy_value': 'margin_buy',
                           'sec_value': 'short_value',
                           'sec_sell_volume': 'short_volume',
                           'fin_sec_value': 'total_value'}, inplace=True)

        df.update(df.select_dtypes(include=[np.number]).fillna(0))

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
    MarginTradingSummaryRecorder(batch_size=30).run()


# the __all__ is generated
__all__ = ['MarginTradingSummaryRecorder']
