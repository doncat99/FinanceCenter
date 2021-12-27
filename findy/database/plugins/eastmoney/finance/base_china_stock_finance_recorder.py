# -*- coding: utf-8 -*-
import time

import pandas as pd

from findy import findy_config
from findy.interface import EntityType
from findy.database.schema.fundamental.finance import FinanceFactor
from findy.database.plugins.eastmoney.common import company_type_flag, get_fc, \
                                                           EastmoneyTimestampsDataRecorder, \
                                                           call_eastmoney_api, get_from_path_fields
from findy.database.context import get_db_session
from findy.utils.pd import pd_valid, index_df
from findy.utils.time import to_time_str, to_pd_timestamp


class BaseChinaStockFinanceRecorder(EastmoneyTimestampsDataRecorder):
    finance_report_type = None
    data_type = 1

    timestamps_fetching_url = 'https://emh5.eastmoney.com/api/CaiWuFenXi/GetCompanyReportDateList'
    timestamp_list_path_fields = ['CompanyReportDateList']
    timestamp_path_fields = ['ReportDate']

    def __init__(self, entity_type=EntityType.Stock, exchanges=['sh', 'sz'],
                 entity_ids=None, codes=None, batch_size=10,
                 force_update=False, sleeping_time=5, default_size=findy_config['batch_size'], real_time=False,
                 fix_duplicate_way='add', start_timestamp=None, end_timestamp=None, close_hour=0,
                 close_minute=0, share_para=None) -> None:
        super().__init__(entity_type, exchanges, entity_ids, codes, batch_size, force_update,
                         sleeping_time, default_size, real_time, fix_duplicate_way, start_timestamp,
                         end_timestamp, close_hour, close_minute, share_para=share_para)

        try:
            self.fetch_jq_timestamp = True
        except Exception as e:
            self.fetch_jq_timestamp = False
            self.logger.warning(f'joinquant account not ok,the timestamp(publish date) for finance would be not correct {e}')

    def init_timestamps(self, entity, http_session):
        param = {
            "color": "w",
            "fc": get_fc(entity),
            "DataType": self.data_type
        }

        if self.finance_report_type == 'LiRunBiaoList' or self.finance_report_type == 'XianJinLiuLiangBiaoList':
            param['ReportType'] = 1

        timestamp_json_list = call_eastmoney_api(http_session, url=self.timestamps_fetching_url,
                                                 path_fields=self.timestamp_list_path_fields,
                                                 param=param)

        if timestamp_json_list is not None and self.timestamp_path_fields:
            timestamps = [get_from_path_fields(data, self.timestamp_path_fields) for data in timestamp_json_list]
        else:
            return []

        return [to_pd_timestamp(t) for t in timestamps]

    def generate_request_param(self, security_item, start, end, size, timestamps, http_session):
        comp_type = company_type_flag(security_item, http_session)

        if len(timestamps) <= 10:
            param = {
                "color": "w",
                "fc": get_fc(security_item),
                "corpType": comp_type,
                # 0 means get all types
                "reportDateType": 0,
                "endDate": '',
                "latestCount": size
            }
        else:
            param = {
                "color": "w",
                "fc": get_fc(security_item),
                "corpType": comp_type,
                # 0 means get all types
                "reportDateType": 0,
                "endDate": to_time_str(timestamps[10]),
                "latestCount": 10
            }

        if self.finance_report_type == 'LiRunBiaoList' or self.finance_report_type == 'XianJinLiuLiangBiaoList':
            param['reportType'] = 1

        return param

    def generate_path_fields(self, security_item, http_session):
        comp_type = company_type_flag(security_item, http_session)

        if comp_type == "3":
            return [f'{self.finance_report_type}_YinHang']
        elif comp_type == "2":
            return [f'{self.finance_report_type}_BaoXian']
        elif comp_type == "1":
            return [f'{self.finance_report_type}_QuanShang']
        elif comp_type == "4":
            return [f'{self.finance_report_type}_QiYe']

    async def record(self, entity, http_session, db_session, para):
        start_point = time.time()

        (ref_record, start, end, size, timestamps) = para
        # different with the default timestamps handling
        param = await self.generate_request_param(entity, start, end, size, timestamps, http_session)
        self.logger.info(f'request param:{param}')

        result = await self.api_wrapper.request(http_session, url=self.url, param=param,
                                                method=self.request_method,
                                                path_fields=self.generate_path_fields(entity, http_session))
        return False, time.time() - start_point, (ref_record, pd.DataFrame.from_records(result))

    def get_original_time_field(self):
        return 'ReportDate'

    async def on_finish_entity(self, entity, http_session, db_session):
        total_time = await super().on_finish_entity(entity, http_session, db_session)

        if not self.fetch_jq_timestamp:
            return total_time

        now = time.time()

        # fill the timestamp for report published date
        the_data_list, column_names = self.data_schema.query_data(
            region=self.region,
            provider=self.provider,
            db_session=db_session,
            entity_id=entity.id,
            order=self.data_schema.timestamp.asc(),
            filters=[self.data_schema.timestamp == self.data_schema.report_date])

        if the_data_list and len(the_data_list) > 0:
            data, column_names = FinanceFactor.query_data(
                region=self.region,
                provider=self.provider,
                db_session=get_db_session(self.region, self.provider, FinanceFactor),
                entity_id=entity.id,
                columns=[
                    FinanceFactor.timestamp,
                    FinanceFactor.report_date,
                    FinanceFactor.id],
                filters=[
                    FinanceFactor.timestamp != FinanceFactor.report_date,
                    FinanceFactor.report_date >= the_data_list[0].report_date,
                    FinanceFactor.report_date <= the_data_list[-1].report_date])

            if data and len(data) > 0:
                df = pd.DataFrame(data, columns=column_names)
                df = index_df(df, index='report_date', time_field='report_date')

                if pd_valid(df):
                    for the_data in the_data_list:
                        if the_data.report_date in df.index:
                            the_data.timestamp = df.at[the_data.report_date, 'timestamp']
                            self.logger.info(
                                'db fill {} {} timestamp:{} for report_date:{}'.format(
                                    self.data_schema.__name__, entity.id,
                                    the_data.timestamp, the_data.report_date))
                    db_session.commit()

        return total_time + time.time() - now
