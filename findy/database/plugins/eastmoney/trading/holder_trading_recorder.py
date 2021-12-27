# -*- coding: utf-8 -*-
from datetime import datetime

import pandas as pd
import numpy as np

from findy.database.schema.fundamental.trading import HolderTrading
from findy.database.plugins.eastmoney.common import EastmoneyMoreDataRecorder
from findy.utils.time import PD_TIME_FORMAT_DAY
from findy.utils.convert import to_float


class HolderTradingRecorder(EastmoneyMoreDataRecorder):
    data_schema = HolderTrading

    url = 'https://emh5.eastmoney.com/api/JiaoYiShuJu/GetGuDongZengJian'
    path_fields = ['GuDongZengJianList']

    def get_original_time_field(self):
        return 'RiQi'

    def generate_domain_id(self, entity, df, time_fmt=PD_TIME_FORMAT_DAY):
        return entity.id + '_' + df[self.get_evaluated_time_field()].dt.strftime(time_fmt) + '_' + df['GuDongMingCheng']

    def format(self, entity, df):
        df['volume'] = df['BianDongShuLiang'].apply(lambda x: to_float(x))
        df['change_pct'] = df['BianDongBiLi'].apply(lambda x: to_float(x))
        df['holding_pct'] = df['BianDongHouChiGuBiLi'].apply(lambda x: to_float(x))

        df.update(df.select_dtypes(include=[np.number]).fillna(0))

        df['holder_name'] = df['GuDongMingCheng'].astype(str)
        df['holder_name'] = df['holder_name'].apply(lambda x: x.replace('\n', '').replace('\r', ''))

        if 'timestamp' not in df.columns:
            df['timestamp'] = pd.to_datetime(df[self.get_original_time_field()])
        elif not isinstance(df['timestamp'].dtypes, datetime):
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        df['entity_id'] = entity.id
        df['provider'] = self.provider.value
        df['code'] = entity.code

        df['id'] = self.generate_domain_id(entity, df)
        return df
