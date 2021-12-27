# -*- coding: utf-8 -*-
import logging
import time

import demjson
import pandas as pd

from findy.interface import Region, Provider
from findy.database.schema import ReportPeriod, CompanyType
from findy.database.schema.meta.stock_meta import StockDetail
from findy.database.plugins.recorder import TimestampsDataRecorder, TimeSeriesDataRecorder
from findy.utils.time import to_pd_timestamp, PD_TIME_FORMAT_DAY
from findy.utils.pd import pd_valid

logger = logging.getLogger(__name__)


def to_report_period_type(report_date):
    the_date = to_pd_timestamp(report_date)
    if the_date.month == 3 and the_date.day == 31:
        return ReportPeriod.season1.value
    if the_date.month == 6 and the_date.day == 30:
        return ReportPeriod.half_year.value
    if the_date.month == 9 and the_date.day == 30:
        return ReportPeriod.season3.value
    if the_date.month == 12 and the_date.day == 31:
        return ReportPeriod.year.value
    return None


def to_jq_report_period(timestamp):
    the_date = to_pd_timestamp(timestamp)
    report_period = to_report_period_type(timestamp)
    if report_period == ReportPeriod.year.value:
        return f'{the_date.year}'
    if report_period == ReportPeriod.season1.value:
        return f'{the_date.year}q1'
    if report_period == ReportPeriod.half_year.value:
        return f'{the_date.year}q2'
    if report_period == ReportPeriod.season3.value:
        return f'{the_date.year}q3'
    assert False


def get_fc(security_item):
    if security_item.exchange == 'sh':
        fc = f"{security_item.code}01"
    if security_item.exchange == 'sz':
        fc = f"{security_item.code}02"
    return fc


def get_company_type(stock_domain: StockDetail):
    if stock_domain.industry is None:
        return CompanyType.qiye
    industry = stock_domain.industry.split(',')
    if ('银行' in industry) or ('信托' in industry):
        return CompanyType.yinhang
    if '保险' in industry:
        return CompanyType.baoxian
    if '证券' in industry:
        return CompanyType.quanshang
    return CompanyType.qiye


async def company_type_flag(security_item, http_session):
    try:
        company_type = get_company_type(security_item)

        if company_type == CompanyType.qiye:
            return "4"
        if company_type == CompanyType.quanshang:
            return "1"
        if company_type == CompanyType.baoxian:
            return "2"
        if company_type == CompanyType.yinhang:
            return "3"
    except Exception as e:
        logger.warning(f"get company_type_flag error: {e}")

    params = {
        "color": "w",
        "fc": get_fc(security_item)
    }

    url = 'https://emh5.eastmoney.com/api/CaiWuFenXi/GetCompanyType'

    async with http_session.get(url, params=params) as response:
        text = await response.text()
        if text is None:
            return {}

        ct = demjson.decode(text)['CompanyType']
        logger.warning(f"{security_item} not catching company type:{ct}")
        return ct


async def call_eastmoney_api(http_session, url=None, method='post', params=None, path_fields=None):
    if method == 'post':
        async with http_session.get(url, params=params) as response:
            text = await response.text()
            if text is None:
                return {}

            json_result = demjson.decode(text)

            if path_fields:
                the_data = get_from_path_fields(json_result, path_fields)
                # if not the_data:
                #     logger.warning(
                #         f"url: {url}, param: {param}, origin_result: {json_result}, could not get data for nested_fields: {path_fields}")
                return the_data

            return json_result
    return {}


def get_from_path_fields(the_json, path_fields):
    the_data = the_json.get(path_fields[0])
    if the_data:
        for field in path_fields[1:]:
            the_data = the_data.get(field)
            if not the_data:
                return None
    return the_data


class ApiWrapper(object):
    async def request(self, url=None, method='post', param=None, path_fields=None):
        raise NotImplementedError


class EastmoneyApiWrapper(ApiWrapper):
    async def request(self, http_session, url=None, method='post', param=None, path_fields=None):
        return await call_eastmoney_api(http_session, url=url, method=method, param=param, path_fields=path_fields)


class BaseEastmoneyRecorder(object):
    request_method = 'post'
    path_fields = None
    api_wrapper = EastmoneyApiWrapper()

    def generate_request_param(self, security_item, start, end, size, timestamp, http_session):
        raise NotImplementedError

    def format(self, entity, df):
        raise NotImplementedError

    async def record(self, entity, http_session, db_session, para):
        start_point = time.time()

        (ref_record, start, end, size, timestamps) = para

        if timestamps:
            original_list = []
            for the_timestamp in timestamps:
                param = self.generate_request_param(entity, start, end, size, the_timestamp, http_session)
                tmp_list = None
                try:
                    tmp_list = await self.api_wrapper.request(http_session, url=self.url, param=param,
                                                              method=self.request_method,
                                                              path_fields=self.path_fields)
                except Exception as e:
                    self.logger.error(f"url: {self.url}, error: {e}")

                if tmp_list is None:
                    continue

                # fill timestamp field
                for tmp in tmp_list:
                    tmp[self.get_evaluated_time_field()] = the_timestamp
                original_list += tmp_list
                if len(original_list) == self.batch_size:
                    break
            return False, time.time() - start_point, (ref_record, pd.DataFrame.from_records(original_list))

        else:
            param = self.generate_request_param(entity, start, end, size, None, http_session)
            try:
                result = await self.api_wrapper.request(http_session, url=self.url, param=param,
                                                        method=self.request_method,
                                                        path_fields=self.path_fields)
                df = pd.DataFrame.from_records(result)

                if pd_valid(df):
                    timefield = self.get_original_time_field()
                    df[timefield] = pd.to_datetime(df[timefield], format=PD_TIME_FORMAT_DAY)
                    return False, time.time() - start_point, (ref_record, self.format(entity, df))

            except Exception as e:
                self.logger.error(f"url: {self.url}, error: {e}")

        return True, time.time() - start_point, None


class EastmoneyTimestampsDataRecorder(BaseEastmoneyRecorder, TimestampsDataRecorder):
    region = Region.CHN
    provider = Provider.EastMoney
    entity_schema = StockDetail

    timestamps_fetching_url = None
    timestamp_list_path_fields = None
    timestamp_path_fields = None

    def init_timestamps(self, entity, http_session):
        param = {
            "color": "w",
            "fc": get_fc(entity)
        }

        timestamp_json_list = call_eastmoney_api(http_session, url=self.timestamps_fetching_url,
                                                 path_fields=self.timestamp_list_path_fields,
                                                 param=param)

        if self.timestamp_path_fields and timestamp_json_list:
            timestamps = [get_from_path_fields(data, self.timestamp_path_fields) for data in timestamp_json_list]
            return [to_pd_timestamp(t) for t in timestamps]
        return []


class EastmoneyPageabeDataRecorder(BaseEastmoneyRecorder, TimeSeriesDataRecorder):
    region = Region.CHN
    provider = Provider.EastMoney
    entity_schema = StockDetail

    page_url = None

    def get_remote_count(self, security_item, http_session):
        param = {
            "color": "w",
            "fc": get_fc(security_item),
            "pageNum": 1,
            "pageSize": 1
        }
        result = call_eastmoney_api(http_session, self.page_url, param=param, path_fields=['TotalCount'])
        if isinstance(result, dict):
            return 0
        else:
            return result

    def eval_fetch_timestamps(self, entity, referenced_record, http_session):
        remote_count = self.get_remote_count(entity, http_session)

        if remote_count == 0:
            return None, None, 0, None

        # get local count
        local_count = len(referenced_record)

        # FIXME:the > case
        size = remote_count - local_count

        if size <= 0:
            return None, None, 0, None

        return None, None, size, None

    def generate_request_param(self, security_item, start, end, size, timestamp, http_session):
        return {
            "color": "w",
            "fc": get_fc(security_item),
            'pageNum': 1,
            # just get more for some fixed data
            'pageSize': size + 10
        }


class EastmoneyMoreDataRecorder(BaseEastmoneyRecorder, TimeSeriesDataRecorder):
    region = Region.CHN
    provider = Provider.EastMoney
    entity_schema = StockDetail

    def get_remote_latest_record(self, security_item, http_session):
        param = {
            "color": "w",
            "fc": get_fc(security_item),
            "pageNum": 1,
            "pageSize": 1
        }
        results = call_eastmoney_api(http_session, self.url, param=param, path_fields=self.path_fields)
        if len(results) > 0:
            df = pd.DataFrame.from_records(results)
            df['timestamp'] = pd.to_datetime(df[self.get_original_time_field()])
            df['id'] = self.generate_domain_id(security_item, df)
            return df.loc[df[self.get_evaluated_time_field()].idxmax()]
        return None

    def eval_fetch_timestamps(self, entity, referenced_record, http_session):
        # get latest record
        latest_record = None
        try:
            if pd_valid(referenced_record):
                latest_record = referenced_record.loc[referenced_record[self.get_evaluated_time_field()].idxmax()]
        except Exception as e:
            self.logger.warning(f"get referenced_record failed with error: {e}")

        if latest_record is not None:
            remote_record = self.get_remote_latest_record(entity, http_session)
            if remote_record is None or (latest_record.index == remote_record.id):
                return None, None, 0, None
            else:
                return None, None, 10, None

        return None, None, 1000, None

    def generate_request_param(self, security_item, start, end, size, timestamp, http_session):
        return {
            "color": "w",
            "fc": get_fc(security_item),
            'pageNum': 1,
            'pageSize': size
        }
