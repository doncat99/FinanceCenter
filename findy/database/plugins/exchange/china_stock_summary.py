from datetime import datetime
import time

import demjson
import pandas as pd

from findy import findy_config
from findy.interface import Region, Provider, EntityType
from findy.database.schema.meta.stock_meta import Index
from findy.database.schema.misc.overall import StockSummary
from findy.database.plugins.recorder import TimestampsDataRecorder
from findy.utils.request import chrome_copy_header_to_dict
from findy.utils.time import to_time_str, now_pd_timestamp
from findy.utils.convert import to_float

DEFAULT_SH_SUMMARY_HEADER = chrome_copy_header_to_dict('''
Host: query.sse.com.cn
Connection: keep-alive
User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36
Accept: */*
Referer: http://www.sse.com.cn/market/stockdata/overview/day/
Accept-Encoding: gzip, deflate
Accept-Language: zh-CN,zh;q=0.8,en;q=0.6
Cookie: yfx_c_g_u_id_10000042=_ck17122009304714819234313401740; VISITED_COMPANY_CODE=%5B%22000016%22%5D; VISITED_INDEX_CODE=%5B%22000016%22%5D; yfx_f_l_v_t_10000042=f_t_1513733447386__r_t_1515716891222__v_t_1515721033042__r_c_3; VISITED_MENU=%5B%228464%22%2C%229666%22%2C%229668%22%2C%229669%22%2C%228454%22%2C%228460%22%2C%229665%22%2C%228459%22%2C%229692%22%2C%228451%22%2C%228466%22%5D
''')


class StockSummaryRecorder(TimestampsDataRecorder):
    region = Region.CHN
    provider = Provider.Exchange
    entity_schema = Index
    data_schema = StockSummary

    url = 'http://query.sse.com.cn/marketdata/tradedata/queryTradingByProdTypeData.do?jsonCallBack=jsonpCallback30731&searchDate={}&prodType=gp&_=1515717065511'

    def __init__(self, exchanges=['cn'], entity_ids=None, codes=['000001'], batch_size=10,
                 force_update=False, sleeping_time=5, default_size=findy_config['batch_size'],
                 real_time=False, fix_duplicate_way='add', share_para=None) -> None:
        super().__init__(EntityType.Index, exchanges, entity_ids, codes, batch_size,
                         force_update, sleeping_time, default_size, real_time,
                         fix_duplicate_way, share_para=share_para)

    def init_timestamps(self, entity, http_session):
        return pd.date_range(start=entity.timestamp,
                             end=now_pd_timestamp(Region.CHN),
                             freq='B').tolist()

    async def record(self, entity, http_session, db_session, para):
        start_point = time.time()

        (ref_record, start, end, size, timestamps) = para

        json_results = []

        for timestamp in timestamps:
            timestamp_str = to_time_str(timestamp)
            url = self.url.format(timestamp_str)

            async with http_session.get(url, headers=DEFAULT_SH_SUMMARY_HEADER) as response:
                if response.status != 200:
                    return

                text = await response.text()
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
                        df = self.format(df)
                        return False, time.time() - start_point, (ref_record, df)

        if len(json_results) > 0:
            df = pd.DataFrame.from_records(json_results)
            df = self.format(df)
            return False, time.time() - start_point, (ref_record, df)

        return True, time.time() - start_point, None

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

    async def on_finish_entity(self, entity, http_session, db_session, result):
        return 0

    async def on_finish(self):
        pass
