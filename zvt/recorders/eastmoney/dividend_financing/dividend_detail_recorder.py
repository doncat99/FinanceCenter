# -*- coding: utf-8 -*-
from datetime import datetime

import pandas as pd
import numpy as np

from zvt.api.data_type import Region, Provider
from zvt.domain import DividendDetail
from zvt.recorders.eastmoney.common import EastmoneyPageabeDataRecorder
from zvt.utils.time_utils import to_pd_timestamp


class DividendDetailRecorder(EastmoneyPageabeDataRecorder):
    data_schema = DividendDetail

    region = Region.CHN
    provider = Provider.EastMoney

    url = 'https://emh5.eastmoney.com/api/FenHongRongZi/GetFenHongSongZhuanList'
    page_url = url
    path_fields = ['FenHongSongZhuanList']

    def get_original_time_field(self):
        return 'GongGaoRiQi'

    def format(self, entity, df):
        # 公告日
        df['announce_date'] = pd.to_datetime(df['GongGaoRiQi'])
        # 股权登记日
        df['record_date'] = df['GuQuanDengJiRi'].apply(lambda x: to_pd_timestamp(x))
        # 除权除息日
        df['dividend_date'] = df['ChuQuanChuXiRi'].apply(lambda x: to_pd_timestamp(x))
        # 方案
        df['dividend'] = df['FengHongFangAn'].astype(str)
        df['dividend'] = df['dividend'].apply(lambda x: x.replace('\n', '').replace('\r', ''))

        df.update(df.select_dtypes(include=[np.number]).fillna(0))

        if 'timestamp' not in df.columns:
            df['timestamp'] = pd.to_datetime(df[self.get_original_time_field()])
        elif not isinstance(df['timestamp'].dtypes, datetime):
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        df['entity_id'] = entity.id
        df['provider'] = self.provider.value
        df['code'] = entity.code

        df['id'] = self.generate_domain_id(entity, df)
        return df


__all__ = ['DividendDetailRecorder']

if __name__ == '__main__':
    # init_log('dividend_detail.log')

    recorder = DividendDetailRecorder(codes=['601318'])
    recorder.run()
