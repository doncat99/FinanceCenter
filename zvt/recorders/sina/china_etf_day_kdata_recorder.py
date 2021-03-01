# -*- coding: utf-8 -*-
from datetime import datetime

import demjson
import pandas as pd

from zvt import zvt_config
from zvt.api.data_type import Region, Provider, EntityType
from zvt.api.quote import get_kdata
from zvt.domain import Etf, Etf1dKdata
from zvt.contract import IntervalLevel
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.contract.api import get_db_session
from zvt.recorders.consts import EASTMONEY_ETF_NET_VALUE_HEADER
from zvt.networking.request import sync_get
from zvt.utils.time_utils import to_time_str
from zvt.utils.pd_utils import pd_is_not_null


class ChinaETFDayKdataRecorder(FixedCycleDataRecorder):
    region = Region.CHN
    provider = Provider.Sina
    entity_schema = Etf
    data_schema = Etf1dKdata

    url = 'http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?' \
          'symbol={}{}&scale=240&&datalen={}&ma=no'

    def __init__(self, entity_type=EntityType.ETF, exchanges=['sh', 'sz'],
                 entity_ids=None, codes=None, batch_size=10, force_update=False,
                 sleeping_time=10, default_size=zvt_config['batch_size'],
                 real_time=True, fix_duplicate_way='add', start_timestamp=None,
                 end_timestamp=None, level=IntervalLevel.LEVEL_1DAY,
                 kdata_use_begin_time=False, close_hour=0, close_minute=0,
                 one_day_trading_minutes=24 * 60, share_para=None) -> None:
        super().__init__(entity_type, exchanges, entity_ids, codes, batch_size,
                         force_update, sleeping_time, default_size, real_time,
                         fix_duplicate_way, start_timestamp, end_timestamp,
                         close_hour, close_minute, level, kdata_use_begin_time,
                         one_day_trading_minutes, share_para=share_para)

    def fetch_cumulative_net_value(self, security_item, start, end, http_session) -> pd.DataFrame:
        query_url = 'http://api.fund.eastmoney.com/f10/lsjz?' \
                    'fundCode={}&pageIndex={}&pageSize=200&startDate={}&endDate={}'

        page = 1
        df = pd.DataFrame()
        while True:
            url = query_url.format(security_item.code, page, to_time_str(start), to_time_str(end))
            text = sync_get(http_session, url, headers=EASTMONEY_ETF_NET_VALUE_HEADER, return_type='text')
            if text is None:
                continue

            response_json = demjson.decode(text)
            response_df = pd.DataFrame(response_json['Data']['LSJZList'])

            # 最后一页
            if not pd_is_not_null(response_df):
                break

            response_df['FSRQ'] = pd.to_datetime(response_df['FSRQ'])
            response_df['JZZZL'] = pd.to_numeric(response_df['JZZZL'], errors='coerce')
            response_df['LJJZ'] = pd.to_numeric(response_df['LJJZ'], errors='coerce')
            response_df = response_df.fillna(0)
            response_df.set_index('FSRQ', inplace=True, drop=True)

            df = pd.concat([df, response_df])
            page += 1

            self.sleep()

        return df

    def record(self, entity, start, end, size, timestamps, http_session):
        # 此 url 不支持分页，如果超过我们想取的条数，则只能取最大条数
        if start is None or size > self.default_size:
            size = 8000

        param = {
            'security_item': entity,
            'level': self.level.value,
            'size': size
        }

        security_item = param['security_item']
        size = param['size']

        url = ChinaETFDayKdataRecorder.url.format(security_item.exchange, security_item.code, size)
        text = sync_get(http_session, url, return_type='text')
        if text is None:
            return None

        response_json = demjson.decode(text)
        if response_json is None or len(response_json) == 0:
            return None

        df = pd.DataFrame(response_json)
        df['level'] = param['level']
        return df

    def format(self, entity, df):
        df.rename(columns={'day': 'timestamp'}, inplace=True)

        if 'timestamp' not in df.columns:
            df['timestamp'] = pd.to_datetime(df[self.get_original_time_field()])
        elif not isinstance(df['timestamp'].dtypes, datetime):
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        df['entity_id'] = entity.id
        df['provider'] = self.provider.value
        df['code'] = entity.code
        df['name'] = entity.name

        df['id'] = self.generate_domain_id(entity, df)
        return df

    def on_finish_entity(self, entity, http_session):
        kdatas = get_kdata(region=self.region,
                           provider=self.provider,
                           entity_id=entity.id,
                           level=IntervalLevel.LEVEL_1DAY.value,
                           order=Etf1dKdata.timestamp.asc(),
                           return_type='domain',
                           filters=[Etf1dKdata.cumulative_net_value.is_(None)])

        if kdatas and len(kdatas) > 0:
            start = kdatas[0].timestamp
            end = kdatas[-1].timestamp

            # 从东方财富获取基金累计净值
            df = self.fetch_cumulative_net_value(entity, start, end, http_session)

            if pd_is_not_null(df):
                for kdata in kdatas:
                    if kdata.timestamp in df.index:
                        kdata.cumulative_net_value = df.loc[kdata.timestamp, 'LJJZ']
                        kdata.change_pct = df.loc[kdata.timestamp, 'JZZZL']
                session = get_db_session(region=self.region,
                                         provider=self.provider,
                                         data_schema=self.data_schema)
                session.commit()
                self.logger.info(f'{entity.code} - {entity.name}累计净值更新完成...')


__all__ = ['ChinaETFDayKdataRecorder']

if __name__ == '__main__':
    from zvt import init_log

    init_log('sina_china_etf_day_kdata.log')
    ChinaETFDayKdataRecorder(level=IntervalLevel.LEVEL_1DAY).run()
