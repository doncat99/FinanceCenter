# -*- coding: utf-8 -*-
from datetime import datetime

import pandas as pd
import numpy as np

from findy.interface import Region, Provider
from findy.database.schema.misc.holder import TopTenHolder
from findy.database.plugins.eastmoney.common import EastmoneyTimestampsDataRecorder, get_fc, to_report_period_type
from findy.utils.time import to_time_str, PD_TIME_FORMAT_DAY
from findy.utils.convert import to_float


class TopTenHolderRecorder(EastmoneyTimestampsDataRecorder):
    region = Region.CHN
    provider = Provider.EastMoney
    data_schema = TopTenHolder

    url = 'https://emh5.eastmoney.com/api/GuBenGuDong/GetShiDaGuDong'
    path_fields = ['ShiDaGuDongList']

    timestamps_fetching_url = 'https://emh5.eastmoney.com/api/GuBenGuDong/GetFirstRequest2Data'
    timestamp_list_path_fields = ['SDGDBGQ', 'ShiDaGuDongBaoGaoQiList']
    timestamp_path_fields = ['BaoGaoQi']

    def generate_request_param(self, security_item, start, end, size, timestamp, http_session):
        return {"color": "w",
                "fc": get_fc(security_item),
                "BaoGaoQi": to_time_str(timestamp)
                }

    def generate_domain_id(self, entity, df, time_fmt=PD_TIME_FORMAT_DAY):
        return entity.id + '_' + df[self.get_evaluated_time_field()].dt.strftime(time_fmt) + '_' + df['holder_name']

    def format(self, entity, df):
        df['report_period'] = df['timestamp'].apply(lambda x: to_report_period_type(x))
        df['report_date'] = pd.to_datetime(df['timestamp'])
        # 股东代码
        df['holder_code'] = df['GuDongDaiMa'].astype(str)
        df['holder_code'] = df['holder_code'].apply(lambda x: x.replace('\n', '').replace('\r', ''))
        # 股东名称
        df['holder_name'] = df['GuDongMingCheng'].astype(str)
        df['holder_name'] = df['holder_name'].apply(lambda x: x.replace('\n', '').replace('\r', ''))

        # 持股数
        df['shareholding_numbers'] = df['ChiGuShu'].apply(lambda x: to_float(x))
        # 持股比例
        df['shareholding_ratio'] = df['ChiGuBiLi'].apply(lambda x: to_float(x))
        # 变动
        df['change'] = df['ZengJian'].apply(lambda x: to_float(x))
        # 变动比例
        df['change_ratio'] = df['BianDongBiLi'].apply(lambda x: to_float(x))

        df.update(df.select_dtypes(include=[np.number]).fillna(0))

        fill_values = {'report_period': "未知",
                       'report_date': pd.to_datetime("1900-01-01"),
                       'holder_name': "未知",
                       'holder_code': "未知"}
        df.fillna(value=fill_values, inplace=True)

        if 'timestamp' not in df.columns:
            df['timestamp'] = pd.to_datetime(df[self.get_original_time_field()])
        elif not isinstance(df['timestamp'].dtypes, datetime):
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        df['entity_id'] = entity.id
        df['provider'] = self.provider.value
        df['code'] = entity.code

        df['id'] = self.generate_domain_id(entity, df)
        return df
