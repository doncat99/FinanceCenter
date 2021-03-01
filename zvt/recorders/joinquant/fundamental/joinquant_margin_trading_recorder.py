# -*- coding: utf-8 -*-
from datetime import datetime

import pandas as pd

from zvt import zvt_config
from zvt.api.data_type import Region, Provider, EntityType
from zvt.domain import Stock, MarginTrading
from zvt.contract.recorder import TimeSeriesDataRecorder
from zvt.recorders.joinquant.common import to_jq_entity_id
from zvt.networking.request import jq_get_mtss
from zvt.utils.pd_utils import pd_is_not_null
from zvt.utils.time_utils import to_time_str


class MarginTradingRecorder(TimeSeriesDataRecorder):
    # 数据来自jq
    region = Region.CHN
    provider = Provider.JoinQuant
    entity_schema = Stock
    data_schema = MarginTrading

    def __init__(self, entity_type=EntityType.Stock, exchanges=None, entity_ids=None,
                 codes=None, batch_size=10, force_update=False, sleeping_time=5,
                 default_size=zvt_config['batch_size'], real_time=False,
                 fix_duplicate_way='add', start_timestamp=None, end_timestamp=None,
                 close_hour=0, close_minute=0, share_para=None) -> None:
        super().__init__(entity_type, exchanges, entity_ids, codes, batch_size,
                         force_update, sleeping_time, default_size, real_time,
                         fix_duplicate_way, start_timestamp, end_timestamp,
                         close_hour, close_minute, share_para=share_para)

    def record(self, entity, start, end, size, timestamps, http_session):
        df = jq_get_mtss(code=to_jq_entity_id(entity), date=to_time_str(start))

        if pd_is_not_null(df):
            return df
        return None

    def format(self, entity, df):
        if 'timestamp' not in df.columns:
            df['timestamp'] = pd.to_datetime(df[self.get_original_time_field()])
        elif not isinstance(df['timestamp'].dtypes, datetime):
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        df.rename(columns={'date': 'timestamp'}, inplace=True)

        df['entity_id'] = entity.id
        df['provider'] = self.provider.value
        df['code'] = entity.code

        df['id'] = self.generate_domain_id(entity, df)
        return df


if __name__ == '__main__':
    MarginTradingRecorder(codes=['000004']).run()


# the __all__ is generated
__all__ = ['MarginTradingRecorder']
