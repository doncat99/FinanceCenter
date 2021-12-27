# -*- coding: utf-8 -*-
from datetime import datetime

import pandas as pd
import numpy as np
from tqdm.auto import tqdm

from findy.interface import Region, Provider
from findy.database.schema.fundamental.dividend_financing import DividendFinancing
from findy.database.plugins.eastmoney.common import EastmoneyPageabeDataRecorder
from findy.database.context import get_db_session
from findy.utils.convert import to_float


class DividendFinancingRecorder(EastmoneyPageabeDataRecorder):
    region = Region.CHN
    provider = Provider.EastMoney
    data_schema = DividendFinancing

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

    async def on_finish(self):
        desc = DividendFinancing.__name__ + ": update relevant table"
        with tqdm(total=len(self.entities), ncols=90, desc=desc, position=2, leave=True) as pbar:
            db_session = get_db_session(self.region, self.provider, DividendFinancing)

            for entity in self.entities:
                code_security = {}
                code_security[entity.code] = entity

                need_fill_items, column_names = DividendFinancing.query_data(
                    region=self.region,
                    provider=self.provider,
                    db_session=db_session,
                    codes=list(code_security.keys()),
                    filters=[
                        DividendFinancing.ipo_raising_fund.is_(None),
                        DividendFinancing.ipo_issues != 0])

                if need_fill_items and len(need_fill_items) > 0:
                    for need_fill_item in need_fill_items:
                        need_fill_item.ipo_raising_fund = code_security[entity.code].raising_fund
                pbar.update()

            db_session.commit()

        await super().on_finish()
