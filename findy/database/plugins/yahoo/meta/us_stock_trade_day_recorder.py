# -*- coding: utf-8 -*-
from datetime import datetime
import time

import pandas as pd
import exchange_calendars as calendars

from findy.interface import Region, Provider, UsExchange
from findy.interface.writer import df_to_db
from findy.database.schema.meta.stock_meta import Stock
from findy.database.schema.quotes.trade_day import StockTradeDay
from findy.database.plugins.recorder import RecorderForEntities
from findy.utils.time import PD_TIME_FORMAT_DAY, to_time_str, now_pd_timestamp


class UsStockTradeDayRecorder(RecorderForEntities):
    region = Region.US
    provider = Provider.Yahoo
    entity_schema = Stock
    data_schema = StockTradeDay

    async def init_entities(self, db_session):
        self.entities = [UsExchange.NYSE.value]

    def generate_domain_id(self, entity, df, time_fmt=PD_TIME_FORMAT_DAY):
        return df['timestamp'].dt.strftime(time_fmt)

    async def eval(self, entity, http_session, db_session):
        return not isinstance(entity, str), 0, None

    async def record(self, entity, http_session, db_session, para):
        start_point = time.time()

        trade_day, column_names = StockTradeDay.query_data(
            region=self.region,
            provider=self.provider,
            db_session=db_session,
            order=StockTradeDay.timestamp.desc(),
            limit=1)

        start = to_time_str(trade_day) if trade_day else "2003-10-11"

        calendar = calendars.get_calendar(entity.upper())
        dates = calendar.sessions_in_range(start, to_time_str(now_pd_timestamp(Region.US)))

        self.logger.info(f'add dates: {dates}')

        if len(dates) > 0:
            df = pd.DataFrame(dates, columns=['timestamp'])
            return False, time.time() - start_point, self.format(entity, df)

        return True, time.time() - start_point, None

    def format(self, entity, df):
        if 'timestamp' not in df.columns:
            df['timestamp'] = pd.to_datetime(df[self.get_original_time_field()])
        elif not isinstance(df['timestamp'].dtypes, datetime):
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        df['entity_id'] = UsExchange.NYSE.value
        df['provider'] = self.provider.value

        df['id'] = self.generate_domain_id(entity, df)
        return df

    async def persist(self, entity, http_session, db_session, df_record):
        start_point = time.time()
        saved = await df_to_db(region=self.region,
                               provider=self.provider,
                               data_schema=self.data_schema,
                               db_session=db_session,
                               df=df_record)
        return True, time.time() - start_point, saved

    async def on_finish_entity(self, entity, http_session, db_session, result):
        return 0

    async def on_finish(self):
        pass
