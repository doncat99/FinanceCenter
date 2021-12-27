# -*- coding: utf-8 -*-
import time

import pandas as pd

from findy import findy_config
from findy.interface import Region, Provider, EntityType
from findy.database.schema import IntervalLevel, AdjustType
from findy.database.schema.meta.stock_meta import Stock
from findy.database.schema.datatype import StockKdataCommon
from findy.database.plugins.recorder import KDataRecorder
from findy.database.plugins.yahoo.common import to_yahoo_trading_level
from findy.database.quote import get_entities
from findy.utils.pd import pd_valid
from findy.utils.time import PD_TIME_FORMAT_DAY, PD_TIME_FORMAT_ISO8601, to_time_str
from findy.vendor.yfinance import YH


class YahooUsStockKdataRecorder(KDataRecorder):
    # 数据来自yahoo
    region = Region.US
    provider = Provider.Yahoo
    entity_schema = Stock
    # 只是为了把recorder注册到data_schema
    data_schema = StockKdataCommon

    def __init__(self,
                 exchanges=['nyse', 'nasdaq', 'amex', 'cme'],
                 entity_ids=None,
                 codes=None,
                 batch_size=10,
                 force_update=True,
                 sleeping_time=0,
                 default_size=findy_config['batch_size'],
                 real_time=False,
                 fix_duplicate_way='ignore',
                 start_timestamp=None,
                 end_timestamp=None,
                 level=IntervalLevel.LEVEL_1WEEK,
                 kdata_use_begin_time=False,
                 close_hour=15,
                 close_minute=0,
                 one_day_trading_minutes=4 * 60,
                 adjust_type=AdjustType.qfq,
                 share_para=None) -> None:
        level = IntervalLevel(level)
        adjust_type = AdjustType(adjust_type)
        self.data_schema = self.get_kdata_schema(entity_type=EntityType.Stock, level=level, adjust_type=adjust_type)
        self.level = level

        super().__init__(EntityType.Stock, exchanges, entity_ids, codes, batch_size, force_update, sleeping_time,
                         default_size, real_time, fix_duplicate_way, start_timestamp, end_timestamp, close_hour,
                         close_minute, level, kdata_use_begin_time, one_day_trading_minutes, share_para=share_para)
        self.adjust_type = adjust_type

    async def init_entities(self, db_session):
        # init the entity list
        self.entities, column_names = get_entities(
            region=self.region,
            provider=self.provider,
            db_session=db_session,
            entity_schema=self.entity_schema,
            entity_type=self.entity_type,
            exchanges=self.exchanges,
            entity_ids=self.entity_ids,
            codes=self.codes,
            filters=[Stock.is_active.is_(True)])

    def generate_domain_id(self, entity, df, time_fmt=PD_TIME_FORMAT_DAY):
        format = PD_TIME_FORMAT_DAY if self.level >= IntervalLevel.LEVEL_1DAY else PD_TIME_FORMAT_ISO8601
        return entity.id + '_' + df[self.get_evaluated_time_field()].dt.strftime(format)

    async def yh_get_bars(self, http_session, entity, start=None, end=None, enable_proxy=False):
        retry = 3
        error_msg = None

        for _ in range(retry):
            try:
                code = entity.code
                if self.level < IntervalLevel.LEVEL_1DAY:
                    df, msg = await YH.fetch(http_session, code, interval=to_yahoo_trading_level(self.level), period="3mon")
                else:
                    df, msg = await YH.fetch(http_session, code, interval=to_yahoo_trading_level(self.level), start=start, end=end)
                if isinstance(msg, str) and "symbol may be delisted" in msg:
                    entity.is_active = False
                return df
            except Exception as e:
                msg = str(e)
                error_msg = f'yh_get_bars, code: {code}, interval: {self.level.value}, error: {msg}'
                if isinstance(msg, str) and ("Server disconnected" in msg or "Cannot connect to host" in msg):
                    self.sleep(60 * 10)
                else:
                    break

        self.logger.error(error_msg)
        return None

    async def record(self, entity, http_session, db_session, para):
        start_point = time.time()

        (ref_record, start, end, size, timestamps) = para

        end_timestamp = to_time_str(self.end_timestamp) if self.end_timestamp else None
        df = await self.yh_get_bars(http_session, entity, start=start, end=end_timestamp)

        if pd_valid(df):
            return False, time.time() - start_point, (ref_record, self.format(entity, df))

        return True, time.time() - start_point, None

    def format(self, entity, df):
        df.reset_index(inplace=True)
        df.rename(columns={'Open': 'open', 'High': 'high', 'Low': 'low',
                           'Close': 'close', 'Adj Close': 'adj_close',
                           'Volume': 'volume', 'Date': 'timestamp',
                           'Datetime': 'timestamp'}, inplace=True)

        # df.update(df.select_dtypes(include=[np.number]).fillna(0))

        df['entity_id'] = entity.id
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['provider'] = Provider.Yahoo.value
        df['level'] = self.level.value
        df['code'] = entity.code
        df['name'] = entity.name

        df['id'] = self.generate_domain_id(entity, df)
        return df

    async def on_finish_entity(self, entity, http_session, db_session, result):
        now = time.time()
        if result == 2 and not entity.is_active:
            db_session.commit()
        return time.time() - now

    async def on_finish(self):
        pass
