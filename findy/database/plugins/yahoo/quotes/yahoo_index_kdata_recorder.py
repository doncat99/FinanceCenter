# -*- coding: utf-8 -*-
import pandas as pd
from yfinance import Ticker

from findy import findy_config
from findy.interface import Region, Provider, UsExchange, EntityType
from findy.database.schema import IntervalLevel, AdjustType
from findy.database.schema.meta.stock_meta import Index
from findy.database.schema.datatype import IndexKdataCommon
from findy.database.recorder import KDataRecorder
from findy.database.plugins.yahoo.common import to_yahoo_trading_level
from findy.utils.functool import time_it
from findy.utils.pd import pd_valid
from findy.utils.time import PD_TIME_FORMAT_DAY, PD_TIME_FORMAT_ISO8601, to_time_str


class YahooUsIndexKdataRecorder(KDataRecorder):
    # 数据来自jq
    region = Region.US
    provider = Provider.Yahoo
    entity_schema = Index
    # 只是为了把recorder注册到data_schema
    data_schema = IndexKdataCommon
    exchanges = [e.value for e in UsExchange]

    def __init__(self,
                 entity_ids=None,
                 codes=None,
                 batch_size=10,
                 force_update=True,
                 sleep_time=0,
                 fix_duplicate_way='ignore',
                 start_timestamp=None,
                 end_timestamp=None,
                 level=IntervalLevel.LEVEL_1WEEK,
                 adjust_type=AdjustType.qfq,
                 share_para=None) -> None:
        level = IntervalLevel(level)
        adjust_type = AdjustType(adjust_type)
        self.data_schema = self.get_kdata_schema(entity_type=EntityType.Index, level=level, adjust_type=adjust_type)
        self.level = level

        super().__init__(EntityType.Stock, entity_ids, codes, batch_size, force_update, sleep_time,
                         fix_duplicate_way, start_timestamp, end_timestamp, level, share_para=share_para)
        self.adjust_type = adjust_type

    def generate_domain_id(self, entity, df, time_fmt=PD_TIME_FORMAT_DAY):
        format = PD_TIME_FORMAT_DAY if self.level >= IntervalLevel.LEVEL_1DAY else PD_TIME_FORMAT_ISO8601
        return entity.id + '_' + df[self.get_evaluated_time_field()].dt.strftime(format)

    async def yh_get_bars(self, http_session, entity, start=None, end=None, enable_proxy=False):
        retry = 3
        error_msg = None
        
        tunnel = findy_config['kuaidaili_proxy_tunnel']
        username = findy_config['kuaidaili_proxy_username']
        password = findy_config['kuaidaili_proxy_password']
        proxies = {
            "http": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password, "proxy": tunnel},
            "https": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password, "proxy": tunnel}
        }

        USE_PROXY = False
        proxies = proxies if USE_PROXY else {}

        for _ in range(retry):
            try:
                code = entity.code
                if self.level < IntervalLevel.LEVEL_1DAY:
                    df = Ticker(code).history(period="3mon", interval=to_yahoo_trading_level(self.level), proxy=proxies, debug=False)
                    # df, msg = await Yahoo.fetch(http_session, 'US/Eastern', code, interval=to_yahoo_trading_level(self.level), period="3mon", proxy=proxies)
                else:
                    df = Ticker(code).history(start=start, end=end, interval=to_yahoo_trading_level(self.level), proxy=proxies, debug=False)
                    # df, msg = await Yahoo.fetch(http_session, 'US/Eastern', code, interval=to_yahoo_trading_level(self.level), start=start, end=end, proxy=proxies)
                return df
            except Exception as e:
                msg = str(e)
                if isinstance(msg, str) and "symbol may be delisted" in msg:
                    entity.is_active = False
                error_msg = f'yh_get_bars, code: {code}, interval: {self.level.value}, error: {msg}'
                if isinstance(msg, str) and ("Server disconnected" in msg or
                                             "Cannot connect to host" in msg or
                                             "Internal Privoxy Error" in msg):
                    await self.sleep(60 * 10)
                else:
                    break

        self.logger.error(error_msg)
        return None

    @time_it
    async def record(self, entity, http_session, db_session, para):
        (start, end, size, timestamps) = para

        end_timestamp = to_time_str(self.end_timestamp) if self.end_timestamp else None
        df = await self.yh_get_bars(http_session, entity, start=start, end=end_timestamp)

        if pd_valid(df):
            return False, self.format(entity, df)

        return True, None

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

    @time_it
    async def on_finish_entity(self, entity, http_session, db_session, result):
        pass

    async def on_finish(self, entities):
        pass
