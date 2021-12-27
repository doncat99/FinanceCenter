# -*- coding: utf-8 -*-
from datetime import datetime

import pandas as pd
import numpy as np

from findy.database.schema.fundamental.trading import ManagerTrading
from findy.database.plugins.eastmoney.common import EastmoneyMoreDataRecorder
from findy.utils.time import PD_TIME_FORMAT_DAY
from findy.utils.convert import to_float


class ManagerTradingRecorder(EastmoneyMoreDataRecorder):
    data_schema = ManagerTrading

    url = 'https://emh5.eastmoney.com/api/JiaoYiShuJu/GetGaoGuanZengJian'
    path_fields = ['GaoGuanZengJianList']

    def get_original_time_field(self):
        return 'RiQi'

    def generate_domain_id(self, entity, df, time_fmt=PD_TIME_FORMAT_DAY):
        return entity.id + '_' + df[self.get_evaluated_time_field()].dt.strftime(time_fmt) + '_' + df['BianDongRen']

    def format(self, entity, df):
        df['volume'] = df['BianDongShuLiang'].apply(lambda x: to_float(x))
        df['price'] = df['JiaoYiJunJia'].apply(lambda x: to_float(x))
        df['holding'] = df['BianDongHouShuLiang'].apply(lambda x: to_float(x))

        df.update(df.select_dtypes(include=[np.number]).fillna(0))

        df['trading_person'] = df['BianDongRen'].astype(str)
        df['trading_person'] = df['trading_person'].apply(lambda x: x.replace('\n', '').replace('\r', ''))

        df['trading_way'] = df['JiaoYiTuJing'].astype(str)
        df['trading_way'] = df['trading_way'].apply(lambda x: x.replace('\n', '').replace('\r', ''))

        df['manager'] = df['GaoGuanMingCheng'].astype(str)
        df['manager'] = df['manager'].apply(lambda x: x.replace('\n', '').replace('\r', ''))

        df['manager_position'] = df['GaoGuanZhiWei'].astype(str)
        df['manager_position'] = df['manager_position'].apply(lambda x: x.replace('\n', '').replace('\r', ''))

        df['relationship_with_manager'] = df['GaoGuanGuanXi'].astype(str)
        df['relationship_with_manager'] = df['relationship_with_manager'].apply(lambda x: x.replace('\n', '').replace('\r', ''))

        if 'timestamp' not in df.columns:
            df['timestamp'] = pd.to_datetime(df[self.get_original_time_field()])
        elif not isinstance(df['timestamp'].dtypes, datetime):
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        df['entity_id'] = entity.id
        df['provider'] = self.provider.value
        df['code'] = entity.code

        df['id'] = self.generate_domain_id(entity, df)
        return df
