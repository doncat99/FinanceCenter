# -*- coding: utf-8 -*-
from datetime import datetime

import pandas as pd

from zvt import zvt_config
from zvt.api.data_type import Region, Provider, EntityType
from zvt.domain import Index, Index1dKdata
from zvt.contract import IntervalLevel
from zvt.contract.recorder import FixedCycleDataRecorder
from zvt.networking.request import sync_get
from zvt.utils.pd_utils import pd_is_not_null
from zvt.utils.time_utils import get_year_quarters, is_same_date, now_pd_timestamp


class ChinaIndexDayKdataRecorder(FixedCycleDataRecorder):
    region = Region.CHN
    provider = Provider.Sina
    entity_schema = Index
    data_schema = Index1dKdata

    url = 'http://vip.stock.finance.sina.com.cn/corp/go.php/vMS_MarketHistory/stockid/{}/type/S.phtml?year={}&jidu={}'

    def __init__(self, entity_type=EntityType.Index, exchanges=['cn'], entity_ids=None,
                 codes=None, batch_size=10, force_update=False, sleeping_time=10,
                 default_size=zvt_config['batch_size'], real_time=False,
                 fix_duplicate_way='add', start_timestamp=None, end_timestamp=None,
                 level=IntervalLevel.LEVEL_1DAY, kdata_use_begin_time=False,
                 close_hour=0, close_minute=0, one_day_trading_minutes=24 * 60) -> None:
        super().__init__(entity_type, exchanges, entity_ids, codes, batch_size,
                         force_update, sleeping_time, default_size, real_time,
                         fix_duplicate_way, start_timestamp, end_timestamp,
                         close_hour, close_minute, level, kdata_use_begin_time,
                         one_day_trading_minutes)

    def record(self, entity, start, end, size, timestamps, http_session):
        the_quarters = get_year_quarters(start, now_pd_timestamp(Region.CHN))
        if not is_same_date(entity.timestamp, start) and len(the_quarters) > 1:
            the_quarters = the_quarters[1:]

        param = {
            'security_item': entity,
            'quarters': the_quarters,
            'level': self.level.value
        }

        security_item = param['security_item']
        quarters = param['quarters']
        level = param['level']

        result_df = pd.DataFrame()
        for year, quarter in quarters:
            query_url = self.url.format(security_item.code, year, quarter)
            text = sync_get(http_session, query_url, encoding='gbk', return_type='text')
            if text is None:
                continue

            try:
                dfs = pd.read_html(text)
            except ValueError as error:
                self.logger.error(f'skip ({year}-{quarter:02d}){security_item.code}{security_item.name}({error})')
                self.sleep()
                continue

            if len(dfs) < 5:
                self.sleep()
                continue

            df = dfs[4].copy()
            df = df.iloc[1:]
            df.columns = ['timestamp', 'open', 'high', 'close', 'low', 'volume', 'turnover']
            result_df = pd.concat([result_df, df])

            self.sleep()

        if pd_is_not_null(result_df):
            result_df['level'] = level
            return result_df
        return None

    def format(self, entity, df):
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


__all__ = ['ChinaIndexDayKdataRecorder']

if __name__ == '__main__':
    ChinaIndexDayKdataRecorder().run()
