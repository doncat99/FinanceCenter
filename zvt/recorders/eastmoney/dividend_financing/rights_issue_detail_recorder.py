# -*- coding: utf-8 -*-
from datetime import datetime

import pandas as pd
import numpy as np
from tqdm.auto import tqdm

from zvt.consts import SAMPLE_STOCK_CODES
from zvt.domain import RightsIssueDetail, DividendFinancing
from zvt.recorders.eastmoney.common import EastmoneyPageabeDataRecorder
from zvt.database.api import get_db_session
from zvt.utils.time_utils import now_pd_timestamp
from zvt.utils.utils import to_float


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

    def on_finish(self):
        last_year = str(now_pd_timestamp(self.region).year)
        codes = [item.code for item in self.entities]
        need_filleds = DividendFinancing.query_data(region=self.region,
                                                    provider=self.provider,
                                                    codes=codes,
                                                    return_type='domain',
                                                    filters=[DividendFinancing.rights_raising_fund.is_(None)],
                                                    end_timestamp=last_year)

        desc = RightsIssueDetail.__name__ + ": update relevant table"
        with tqdm(total=len(need_filleds), ncols=120, desc=desc, position=2, leave=True) as pbar:
            from sqlalchemy import func

            session = get_db_session(region=self.region,
                                     provider=self.provider,
                                     data_schema=self.data_schema)

            for item in need_filleds:
                result = RightsIssueDetail.query_data(
                                                region=self.region,
                                                provider=self.provider,
                                                entity_id=item.entity_id,
                                                start_timestamp=item.timestamp,
                                                end_timestamp="{}-12-31".format(item.timestamp.year),
                                                return_type='func',
                                                func=func.sum(RightsIssueDetail.rights_raising_fund))

                if isinstance(result, (int, float)):
                    item.rights_raising_fund = result
                    session.commit()
                pbar.update()

        super().on_finish()


__all__ = ['RightsIssueDetailRecorder']

if __name__ == '__main__':
    # init_log('rights_issue.log')

    recorder = RightsIssueDetailRecorder(codes=SAMPLE_STOCK_CODES)
    recorder.run()
