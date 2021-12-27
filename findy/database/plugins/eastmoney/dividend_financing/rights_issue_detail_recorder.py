# -*- coding: utf-8 -*-
from datetime import datetime

import pandas as pd
import numpy as np
from tqdm.auto import tqdm
from sqlalchemy import func

from findy.database.schema.fundamental.dividend_financing import RightsIssueDetail, DividendFinancing
from findy.database.plugins.eastmoney.common import EastmoneyPageabeDataRecorder
from findy.database.context import get_db_session
from findy.utils.time import now_pd_timestamp
from findy.utils.convert import to_float


class RightsIssueDetailRecorder(EastmoneyPageabeDataRecorder):
    data_schema = RightsIssueDetail

    url = 'https://emh5.eastmoney.com/api/FenHongRongZi/GetPeiGuMingXiList'
    page_url = url
    path_fields = ['PeiGuMingXiList']

    def get_original_time_field(self):
        return 'PeiGuGongGaoRi'

    def format(self, entity, df):
        df['rights_issues'] = df['ShiJiPeiGu'].apply(lambda x: to_float(x))
        df['rights_issue_price'] = df['PeiGuJiaGe'].apply(lambda x: to_float(x))
        df['rights_raising_fund'] = df['ShiJiMuJi'].apply(lambda x: to_float(x))

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
        last_year = str(now_pd_timestamp(self.region).year)
        codes = [item.code for item in self.entities]

        db_session = get_db_session(self.region, self.provider, DividendFinancing)

        need_filleds, column_names = DividendFinancing.query_data(
            region=self.region,
            provider=self.provider,
            db_session=db_session,
            codes=codes,
            end_timestamp=last_year,
            filters=[DividendFinancing.rights_raising_fund.is_(None)])

        if need_filleds:
            desc = self.data_schema.__name__ + ": update relevant table"
            with tqdm(total=len(need_filleds), ncols=90, desc=desc, position=2, leave=True) as pbar:
                db_session_1 = get_db_session(self.region, self.provider, self.data_schema)
                for item in need_filleds:
                    result, column_names = self.data_schema.query_data(
                        region=self.region,
                        provider=self.provider,
                        db_session=db_session_1,
                        entity_id=item.entity_id,
                        start_timestamp=item.timestamp,
                        end_timestamp=f"{item.timestamp.year}-12-31",
                        func=func.sum(self.data_schema.rights_raising_fund))

                    if isinstance(result, (int, float)):
                        item.rights_raising_fund = result
                    pbar.update()
            db_session.commit()

        await super().on_finish()
