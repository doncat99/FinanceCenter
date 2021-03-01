from datetime import datetime

import pandas as pd
import numpy as np

from zvt import zvt_config
from zvt.api.data_type import Region, Provider, EntityType
from zvt.domain import Index, CrossMarketSummary
from zvt.contract.recorder import TimeSeriesDataRecorder
from zvt.networking.request import jq_run_query
from zvt.utils.time_utils import to_time_str
from zvt.utils.pd_utils import pd_is_not_null


class CrossMarketSummaryRecorder(TimeSeriesDataRecorder):
    region = Region.CHN
    provider = Provider.JoinQuant
    entity_schema = Index
    data_schema = CrossMarketSummary

    def __init__(self, batch_size=10,
                 force_update=False, sleeping_time=5, default_size=zvt_config['batch_size'],
                 real_time=False, fix_duplicate_way='add', share_para=None) -> None:

        # 聚宽编码
        # 市场通编码    市场通名称
        # 310001    沪股通
        # 310002    深股通
        # 310003    港股通（沪）
        # 310004    港股通（深）

        codes = ['310001', '310002', '310003', '310004']
        super().__init__(EntityType.Index, ['sz'], None, codes, batch_size,
                         force_update, sleeping_time,
                         default_size, real_time, fix_duplicate_way, share_para=share_para)

    def init_entities(self):
        super().init_entities()

    def record(self, entity, start, end, size, timestamps, http_session):
        df = jq_run_query(table='finance.STK_ML_QUOTA', conditions=f'link_id#=#{entity.code}&day#>=#{to_time_str(start)}', parse_dates=None)
        if pd_is_not_null(df):
            if len(df) < 100:
                self.one_shot = True
            return df
        return None

    def format(self, entity, df):
        cols = ['day', 'buy_amount', 'buy_volume', 'sell_amount', 'sell_volume', 'quota_daily', 'quota_daily_balance']
        df = df[cols].copy()
        df.rename(columns={'day': 'timestamp'}, inplace=True)

        df.update(df.select_dtypes(include=[np.number]).fillna(0))

        if 'timestamp' not in df.columns:
            df['timestamp'] = pd.to_datetime(df[self.get_original_time_field()])
        elif not isinstance(df['timestamp'].dtypes, datetime):
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        df['buy_amount'] *= 100000000
        df['sell_amount'] *= 100000000
        df['quota_daily'] *= 100000000
        df['quota_daily_balance'] *= 100000000

        df['entity_id'] = entity.id
        df['provider'] = self.provider.value
        df['code'] = entity.code
        df['name'] = entity.name

        df['id'] = self.generate_domain_id(entity, df)
        return df


if __name__ == '__main__':
    CrossMarketSummaryRecorder(batch_size=30).run()


# the __all__ is generated
__all__ = ['CrossMarketSummaryRecorder']
