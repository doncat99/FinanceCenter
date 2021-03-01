from datetime import datetime

import pandas as pd
import numpy as np

from zvt import zvt_config
from zvt.api.data_type import Region, Provider, EntityType
from zvt.domain import Index, StockSummary
from zvt.contract.recorder import TimeSeriesDataRecorder
from zvt.networking.request import jq_run_query
from zvt.utils.time_utils import to_time_str
from zvt.utils.pd_utils import pd_is_not_null


# 聚宽编码
# 322001    上海市场
# 322002    上海A股
# 322003    上海B股
# 322004    深圳市场    该市场交易所未公布成交量和成交笔数
# 322005    深市主板
# 322006    中小企业板
# 322007    创业板

code_map_jq = {
    '000001': '322002',
    '399106': '322004',
    '399001': '322005',
    '399005': '322006',
    '399006': '322007'
}


class StockSummaryRecorder(TimeSeriesDataRecorder):
    region = Region.CHN
    provider = Provider.JoinQuant
    entity_schema = Index
    data_schema = StockSummary

    def __init__(self, batch_size=10, force_update=False, sleeping_time=5,
                 default_size=zvt_config['batch_size'], real_time=False,
                 fix_duplicate_way='add', share_para=None) -> None:
        # 上海A股,深圳市场,深圳成指,中小板,创业板
        codes = ['000001', '399106', '399001', '399005', '399006']
        super().__init__(EntityType.Index, ['sh', 'sz'], None, codes, batch_size,
                         force_update, sleeping_time, default_size, real_time,
                         fix_duplicate_way, share_para=share_para)

    def record(self, entity, start, end, size, timestamps, http_session):
        jq_code = code_map_jq.get(entity.code)

        df = jq_run_query(table='finance.STK_EXCHANGE_TRADE_INFO',
                          conditions=f'exchange_code#=#{jq_code}&date#>=#{to_time_str(start)}',
                          parse_dates=['date'])

        if pd_is_not_null(df):
            if len(df) < 100:
                self.one_shot = True
            return df
        return None

    def format(self, entity, df):
        cols = ['date', 'pe_average', 'total_market_cap', 'circulating_market_cap', 'volume', 'money', 'turnover_ratio']
        df = df[cols].copy()
        df.rename(columns={'date': 'timestamp',
                           'pe_average': 'pe',
                           'total_market_cap': 'total_value',
                           'circulating_market_cap': 'total_tradable_vaule',
                           'money': 'turnover',
                           'turnover_ratio': 'turnover_rate'}, inplace=True)

        df.update(df.select_dtypes(include=[np.number]).fillna(0))

        if 'timestamp' not in df.columns:
            df['timestamp'] = pd.to_datetime(df[self.get_original_time_field()])
        elif not isinstance(df['timestamp'].dtypes, datetime):
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        df['total_value'] *= 100000000
        df['total_tradable_vaule'] *= 100000000
        df['volume'] *= 100000000
        df['turnover'] *= 100000000

        df['entity_id'] = entity.id
        df['provider'] = self.provider.value
        df['code'] = entity.code
        df['name'] = entity.name

        df['id'] = self.generate_domain_id(entity, df)
        return df


if __name__ == '__main__':
    StockSummaryRecorder(batch_size=30).run()


# the __all__ is generated
__all__ = ['StockSummaryRecorder']
