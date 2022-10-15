# -*- coding: utf-8 -*-
import json
from datetime import datetime

import pandas as pd
import numpy as np
from sqlalchemy import func

from findy import findy_config
from findy.database.schema.fundamental.dividend_financing import RightsIssueDetail, DividendFinancing
from findy.database.plugins.eastmoney.common import EastmoneyPageabeDataRecorder
from findy.database.context import get_db_session
from findy.utils.time import now_pd_timestamp
from findy.utils.convert import to_float
from findy.utils.kafka import connect_kafka_producer, publish_message
from findy.utils.progress import progress_topic, progress_key


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

    async def on_finish_entity(self, entity, http_session, db_session, result):
        return 0

    async def on_finish(self, entities):
        last_year = str(now_pd_timestamp(self.region).year)
        codes = [item.code for item in entities]

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

            db_session_1 = get_db_session(self.region, self.provider, self.data_schema)
            kafka_producer = connect_kafka_producer(findy_config['kafka'])

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

                data = {"task": 'rig', "total": len(need_filleds), "desc": desc, "leave": True, "update": 1}
                publish_message(kafka_producer, progress_topic, bytes(progress_key, encoding='utf-8'), bytes(json.dumps(data), encoding='utf-8'))

            try:
                db_session.commit()
            except Exception as e:
                self.logger.error(f'{self.__class__.__name__}, error: {e}')
                db_session.rollback()
            finally:
                db_session.close()

        await super().on_finish(entities)
