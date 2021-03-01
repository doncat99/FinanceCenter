from datetime import datetime

import demjson
import pandas as pd

from zvt import zvt_config
from zvt.api.data_type import Region, Provider, EntityType
from zvt.domain import Index
from zvt.domain.misc import StockSummary
from zvt.contract.recorder import TimestampsDataRecorder
from zvt.recorders.consts import DEFAULT_SH_SUMMARY_HEADER
from zvt.networking.request import sync_get
from zvt.utils.time_utils import to_time_str, now_pd_timestamp
from zvt.utils.utils import to_float


class StockSummaryRecorder(TimestampsDataRecorder):
    region = Region.CHN
    provider = Provider.Exchange
    entity_schema = Index
    data_schema = StockSummary

    url = 'http://query.sse.com.cn/marketdata/tradedata/queryTradingByProdTypeData.do?jsonCallBack=jsonpCallback30731&searchDate={}&prodType=gp&_=1515717065511'

    def __init__(self, exchanges=['cn'], entity_ids=None, codes=['000001'], batch_size=10,
                 force_update=False, sleeping_time=5, default_size=zvt_config['batch_size'],
                 real_time=False, fix_duplicate_way='add') -> None:
        super().__init__(EntityType.Index, exchanges, entity_ids, codes, batch_size,
                         force_update, sleeping_time, default_size, real_time,
                         fix_duplicate_way)

    def init_timestamps(self, entity, http_session):
        return pd.date_range(start=entity.timestamp,
                             end=now_pd_timestamp(Region.CHN),
                             freq='B').tolist()

    def record(self, entity, start, end, size, timestamps, http_session):
        json_results = []
        for timestamp in timestamps:
            timestamp_str = to_time_str(timestamp)
            url = self.url.format(timestamp_str)
            text = sync_get(http_session, url=url, headers=DEFAULT_SH_SUMMARY_HEADER, return_type='text')
            if text is None:
                continue

            results = demjson.decode(text[text.index("(") + 1:text.index(")")])['result']
            result = [result for result in results if result['productType'] == '1']
            if result and len(result) == 1:
                result_json = result[0]
                # 有些较老的数据不存在,默认设为0.0
                json_results.append({
                    'timestamp': timestamp,
                    'pe': to_float(result_json['profitRate'], 0.0),
                    'total_value': to_float(result_json['marketValue1'] + '亿', 0.0),
                    'total_tradable_vaule': to_float(result_json['negotiableValue1'] + '亿', 0.0),
                    'volume': to_float(result_json['trdVol1'] + '万', 0.0),
                    'turnover': to_float(result_json['trdAmt1'] + '亿', 0.0),
                    'turnover_rate': to_float(result_json['exchangeRate'], 0.0),
                })

                if len(json_results) > self.batch_size:
                    df = pd.DataFrame.from_records(json_results)
                    df['entity_id'] = entity.id
                    df['provider'] = Provider.Exchange.value
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df['name'] = '上证指数'
                    return df

        if len(json_results) > 0:

            df = pd.DataFrame.from_records(json_results)
            return df
        return None

    def format(self, entity, df):
        if 'timestamp' not in df.columns:
            df['timestamp'] = pd.to_datetime(df[self.get_original_time_field()])
        elif not isinstance(df['timestamp'].dtypes, datetime):
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        df['entity_id'] = entity.id
        df['provider'] = self.provider.value
        df['name'] = '上证指数'

        df['id'] = self.generate_domain_id(entity, df)
        return df


if __name__ == '__main__':
    StockSummaryRecorder(batch_size=30).run()
