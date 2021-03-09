# -*- coding: utf-8 -*-
from datetime import datetime

import pandas as pd
import numpy as np
from tqdm.auto import tqdm

from zvt.api.data_type import Region, Provider
from zvt.domain.fundamental.dividend_financing import DividendFinancing
from zvt.recorders.eastmoney.common import EastmoneyPageabeDataRecorder
from zvt.database.api import get_db_session
from zvt.utils.utils import to_float


class DividendFinancingRecorder(EastmoneyPageabeDataRecorder):
    data_schema = DividendFinancing

    region = Region.CHN
    provider = Provider.EastMoney

    url = 'https://emh5.eastmoney.com/api/FenHongRongZi/GetLiNianFenHongRongZiList'
    page_url = url
    path_fields = ['LiNianFenHongRongZiList']

    def get_original_time_field(self):
        return 'ShiJian'

    def format(self, entity, df):
        # 分红总额
        df['dividend_money'] = df['FenHongZongE'].apply(lambda x: to_float(x[1]))
        # 新股
        df['ipo_issues'] = df['XinGu'].apply(lambda x: to_float(x[1]))
        # 增发
        df['spo_issues'] = df['ZengFa'].apply(lambda x: to_float(x[1]))
        # 配股
        df['rights_issues'] = df['PeiFa'].apply(lambda x: to_float(x[1]))

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

    def on_finish(self):
        desc = DividendFinancing.__name__ + ": update relevant table"
        with tqdm(total=len(self.entities), ncols=120, desc=desc, position=2, leave=True) as pbar:
            session = get_db_session(region=self.region,
                                     provider=self.provider,
                                     data_schema=self.data_schema)

            for entity in self.entities:
                code_security = {}
                code_security[entity.code] = entity

                need_fill_items = DividendFinancing.query_data(region=self.region,
                                                               provider=self.provider,
                                                               codes=list(code_security.keys()),
                                                               return_type='domain',
                                                               filters=[
                                                                   DividendFinancing.ipo_raising_fund.is_(None),
                                                                   DividendFinancing.ipo_issues != 0])
                for need_fill_item in need_fill_items:
                    need_fill_item.ipo_raising_fund = code_security[entity.code].raising_fund
                    session.commit()
                pbar.update()

        super().on_finish()


__all__ = ['DividendFinancingRecorder']

if __name__ == '__main__':
    # init_log('dividend_financing.log')

    recorder = DividendFinancingRecorder(codes=['000999'])
    recorder.run()
