# -*- coding: utf-8 -*-
import logging
import time
import math
from typing import List, Union
import asyncio

import pandas as pd
from tqdm.auto import tqdm

from findy import findy_config
from findy.interface import Region, Provider, EntityType
from findy.interface.writer import df_to_db
from findy.database.schema import IntervalLevel, AdjustType
from findy.database.schema.datatype import Mixin, EntityMixin
from findy.database.schema.quotes.trade_day import StockTradeDay
from findy.database.plugins.register import get_schema_by_name
from findy.database.context import get_db_session
from findy.database.quote import get_entities
from findy.utils.request import get_http_session
from findy.utils.pd import pd_valid
from findy.utils.time import (PD_TIME_FORMAT_DAY, PRECISION_STR,
                              to_pd_timestamp, to_time_str,
                              now_pd_timestamp, next_dates,
                              is_same_date)


class Meta(type):
    def __new__(meta, name, bases, class_dict):
        cls = type.__new__(meta, name, bases, class_dict)
        # register the recorder class to the data_schema
        if hasattr(cls, 'data_schema') and hasattr(cls, 'provider'):
            if cls.data_schema and issubclass(cls.data_schema, Mixin):
                # print(f'{cls.__name__}:{cls.data_schema.__name__}:{cls.region}:{cls.provider}')
                cls.data_schema.register_recorder_cls(cls.region,
                                                      cls.provider,
                                                      cls)
        # else:
        #     print(cls)
        return cls


class Recorder(metaclass=Meta):
    logger = logging.getLogger(__name__)

    # overwrite them to setup the data you want to record
    region: Region = None
    provider: Provider = None
    data_schema: Mixin = None

    def __init__(self,
                 batch_size: int = 10,
                 force_update: bool = False,
                 sleeping_time: int = 10) -> None:

        self.logger = logging.getLogger(self.__class__.__name__)

        assert self.data_schema is not None
        assert self.provider is not None
        assert self.provider in self.data_schema.providers[self.region]

        self.batch_size = batch_size
        self.force_update = force_update
        self.sleeping_time = sleeping_time

    async def run(self):
        raise NotImplementedError

    async def sleep(self, sleeping_time=0.0):
        sleep_time = max(sleeping_time, self.sleeping_time)
        if sleep_time > 0:
            self.logger.debug(f'sleeping {sleep_time} seconds')
            return await asyncio.sleep(sleep_time)


class RecorderForEntities(Recorder):
    # overwrite them to fetch the entity list
    provider: Provider = None
    entity_schema: EntityMixin = None

    def __init__(self,
                 entity_type: EntityType = EntityType.Stock,
                 exchanges=None,
                 entity_ids=None,
                 codes=None,
                 batch_size=10,
                 force_update=False,
                 sleeping_time=10,
                 share_para=None) -> None:

        super().__init__(batch_size=batch_size,
                         force_update=force_update,
                         sleeping_time=sleeping_time)

        assert self.provider is not None

        # setup the entities you want to record
        self.entity_type = entity_type
        self.exchanges = exchanges
        self.codes = codes
        self.share_para = share_para

        # set entity_ids or (entity_type,exchanges,codes)
        self.entity_ids = entity_ids

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
            codes=self.codes)

    async def eval(self, entity, http_session, db_session):
        raise NotImplementedError

    async def record(self, entity, http_session, db_session, para):
        raise NotImplementedError

    async def persist(self, entity, http_session, db_session, para):
        raise NotImplementedError

    async def on_finish_entity(self, entity, http_session, db_session, result):
        raise NotImplementedError

    async def on_finish(self):
        raise NotImplementedError

    async def process_entity(self, entity, http_session, db_session, throttler):
        eval_time = 0
        download_time = 0
        persist_time = 0

        start_point = time.time()

        # eval
        is_finish, eval_time, para = await self.eval(entity, http_session, db_session)
        if is_finish:
            return 1, eval_time, download_time, persist_time, time.time() - start_point

        async with throttler:
            start_point = time.time()

            # fetch
            is_finish, download_time, para = await self.record(entity, http_session, db_session, para)
            if is_finish:
                return 2, eval_time, download_time, persist_time, time.time() - start_point + eval_time

            # save
            is_finish, persist_time, count = await self.persist(entity, http_session, db_session, para)
            if is_finish:
                await self.sleep()
                return 3, eval_time, download_time, persist_time, time.time() - start_point + eval_time

        return 0, eval_time, download_time, persist_time, time.time() - start_point + eval_time

    async def process_loop(self, entity, http_session, db_session, throttler):
        eval_time = 0
        download_time = 0
        persist_time = 0
        total_time = 0

        self.result = None

        while True:
            result, eval_, download_, persist_, total_ = await self.process_entity(entity, http_session, db_session, throttler)
            eval_time += eval_
            download_time += download_
            persist_time += persist_
            total_time += total_

            if result > 0:
                # add finished entity to finished_items
                total_time += await self.on_finish_entity(entity, http_session, db_session, result)
                break

        eval_time = PRECISION_STR.format(eval_time)
        download_time = PRECISION_STR.format(download_time)
        persist_time = PRECISION_STR.format(persist_time)
        total_time = PRECISION_STR.format(total_time)

        prefix = "finish~ " if findy_config['debug'] else ""
        postfix = "\n" if findy_config['debug'] else ""

        name = entity if isinstance(entity, str) else entity.id
        if self.result is not None:
            self.logger.info("{}{:>17}, {:>18}, eval: {}, download: {}, persist: {}, total: {}, size: {:>7}, date: [ {}, {} ]{}".format(
                prefix, self.data_schema.__name__, name, eval_time, download_time, persist_time, total_time,
                self.result[0], self.result[1], self.result[2], postfix))
        else:
            self.logger.info("{}{:>17}, {:>18}, eval: {}, download: {}, persist: {}, total: {}{}".format(
                prefix, self.data_schema.__name__, name, eval_time, download_time, persist_time, total_time, postfix))

    async def run(self):
        db_session = get_db_session(self.region, self.provider, self.data_schema)

        if not hasattr(self, 'entities'):
            self.entities: List = None
            await self.init_entities(db_session)

        if self.entities and len(self.entities) > 0:
            http_session = get_http_session()
            throttler = asyncio.Semaphore(self.share_para[0])

            # tasks = [asyncio.ensure_future(self.process_loop(entity, http_session, throttler)) for entity in self.entities]
            tasks = [self.process_loop(entity, http_session, db_session, throttler) for entity in self.entities]

            (index, desc) = self.share_para[1]

            pbar = tqdm(total=len(tasks), ncols=90, position=index, desc=f"  {desc}", leave=True)
            for result in asyncio.as_completed(tasks):
                await result
                pbar.update()

            # [await _ for _ in tqdm.as_completed(tasks, ncols=90, position=index, desc=f"  {desc}", leave=True)]

            await self.on_finish()

            return await http_session.close()


class TimeSeriesDataRecorder(RecorderForEntities):
    def __init__(self,
                 entity_type: EntityType = EntityType.Stock,
                 exchanges=None,
                 entity_ids=None,
                 codes=None,
                 batch_size=10,
                 force_update=False,
                 sleeping_time=5,
                 default_size=findy_config['batch_size'],
                 real_time=False,
                 fix_duplicate_way='add',
                 start_timestamp=None,
                 end_timestamp=None,
                 close_hour=0,
                 close_minute=0,
                 share_para=None) -> None:
        self.default_size = default_size
        self.real_time = real_time

        self.close_hour = close_hour
        self.close_minute = close_minute

        self.fix_duplicate_way = fix_duplicate_way
        self.start_timestamp = to_pd_timestamp(start_timestamp)
        self.end_timestamp = to_pd_timestamp(end_timestamp)

        super().__init__(entity_type, exchanges, entity_ids, codes, batch_size,
                         force_update, sleeping_time, share_para=share_para)

    def get_evaluated_time_field(self):
        return 'timestamp'

    def get_original_time_field(self):
        return 'timestamp'

    def generate_domain_id(self, entity, df, time_fmt=PD_TIME_FORMAT_DAY):
        time_field = self.get_evaluated_time_field()
        return entity.id + '_' + df[time_field].dt.strftime(time_fmt)

    async def get_referenced_saved_record(self, entity, db_session):
        data, column_names = self.data_schema.query_data(
            region=self.region,
            provider=self.provider,
            db_session=db_session,
            entity_id=entity.id,
            columns=['id', self.get_evaluated_time_field()])
        return pd.DataFrame(data, columns=column_names)

    def eval_fetch_timestamps(self, entity, ref_record, http_session):
        latest_timestamp = None
        try:
            if pd_valid(ref_record):
                time_field = self.get_evaluated_time_field()
                latest_timestamp = ref_record[time_field].max(axis=0)
        except Exception as e:
            self.logger.warning("get ref_record failed with error: {}".format(e))

        if not latest_timestamp:
            latest_timestamp = entity.timestamp

        if not latest_timestamp:
            return self.start_timestamp, self.end_timestamp, self.default_size, None

        if latest_timestamp.date() >= now_pd_timestamp(self.region).date():
            return latest_timestamp, None, 0, None

        if len(self.trade_day) > 0 and \
           latest_timestamp.date() >= self.trade_day[0].date():
            return latest_timestamp, None, 0, None

        if self.start_timestamp:
            latest_timestamp = max(latest_timestamp, self.start_timestamp)

        if self.end_timestamp and latest_timestamp > self.end_timestamp:
            size = 0
        else:
            size = self.default_size

        return latest_timestamp, self.end_timestamp, size, None

    async def eval(self, entity, http_session, db_session):
        start_point = time.time()

        ref_record = await self.get_referenced_saved_record(entity, db_session)

        cost = PRECISION_STR.format(time.time() - start_point)
        self.logger.debug(f'get latest saved record: {len(ref_record)}, cost: {cost}')

        start, end, size, timestamps = \
            self.eval_fetch_timestamps(entity, ref_record, http_session)

        cost = PRECISION_STR.format(time.time() - start_point)
        self.logger.debug(f'evaluate entity: {entity.id}, time: {cost}')

        # no more to record
        is_finish = True if size == 0 else False

        return is_finish, time.time() - start_point, (ref_record, start, end, size, timestamps)

    async def persist(self, entity, http_session, db_session, para):
        start_point = time.time()

        (ref_record, df_record) = para
        saved_counts = 0
        is_finished = False

        if pd_valid(df_record):
            assert 'id' in df_record.columns
            saved_counts = await df_to_db(region=self.region,
                                          provider=self.provider,
                                          data_schema=self.data_schema,
                                          db_session=db_session,
                                          df=df_record,
                                          ref_df=ref_record,
                                          fix_duplicate_way=self.fix_duplicate_way)
            if saved_counts == 0:
                is_finished = True

        # could not get more data
        else:
            # not realtime
            if not self.real_time:
                is_finished = True

            # realtime and to the close time
            elif (self.close_hour is not None) and (self.close_minute is not None):
                now = now_pd_timestamp(self.region)
                if now.hour >= self.close_hour:
                    if now.minute - self.close_minute >= 5:
                        self.logger.info(f'{entity.id} now is the close time: {now}')
                        is_finished = True

        if isinstance(self, KDataRecorder):
            is_finished = True

        start_timestamp = to_time_str(df_record['timestamp'].min(axis=0))
        end_timestamp = to_time_str(df_record['timestamp'].max(axis=0))

        self.result = [saved_counts, start_timestamp, end_timestamp]

        return is_finished, time.time() - start_point, saved_counts

    async def run(self):
        db_session = get_db_session(self.region, self.provider, self.data_schema)
        trade_days, column_names = StockTradeDay.query_data(
            region=self.region,
            provider=self.provider,
            db_session=db_session,
            order=StockTradeDay.timestamp.desc())

        if trade_days and len(trade_days) > 0:
            self.trade_day = [day.timestamp for day in trade_days]
        else:
            self.trade_day = []
            self.logger.warning("load trade days failed")

        return await super().run()


class KDataRecorder(TimeSeriesDataRecorder):
    def __init__(self,
                 entity_type: EntityType = EntityType.Stock,
                 exchanges=None,
                 entity_ids=None,
                 codes=None,
                 batch_size=10,
                 force_update=True,
                 sleeping_time=10,
                 default_size=findy_config['batch_size'],
                 real_time=False,
                 fix_duplicate_way='ignore',
                 start_timestamp=None,
                 end_timestamp=None,
                 close_hour=0,
                 close_minute=0,
                 # child add
                 level=IntervalLevel.LEVEL_1DAY,
                 kdata_use_begin_time=False,
                 one_day_trading_minutes=24 * 60,
                 share_para=None):
        super().__init__(entity_type, exchanges, entity_ids, codes, batch_size,
                         force_update, sleeping_time, default_size, real_time,
                         fix_duplicate_way, start_timestamp, end_timestamp,
                         close_hour, close_minute, share_para=share_para)

        self.level = IntervalLevel(level)
        self.kdata_use_begin_time = kdata_use_begin_time
        self.one_day_trading_minutes = one_day_trading_minutes

    @staticmethod
    def get_kdata_schema(entity_type: EntityType,
                         level: Union[IntervalLevel, str] = IntervalLevel.LEVEL_1DAY,
                         adjust_type: Union[AdjustType, str] = None):
        if type(level) == str:
            level = IntervalLevel(level)
        if type(adjust_type) == str:
            adjust_type = AdjustType(adjust_type)

        # kdata schema rule
        # 1)name:{SecurityType.value.capitalize()}{IntervalLevel.value.upper()}Kdata
        if adjust_type and (adjust_type != AdjustType.qfq):
            schema_str = f'{entity_type.value.capitalize()}{level.value.capitalize()}{adjust_type.value.capitalize()}Kdata'
        else:
            schema_str = f'{entity_type.value.capitalize()}{level.value.capitalize()}Kdata'
        return get_schema_by_name(schema_str)

    def eval_size_of_timestamp(self,
                               start_timestamp: pd.Timestamp,
                               end_timestamp: pd.Timestamp,
                               level: IntervalLevel,
                               one_day_trading_minutes):
        assert end_timestamp is not None

        time_delta = end_timestamp - to_pd_timestamp(start_timestamp)

        one_day_trading_seconds = one_day_trading_minutes * 60

        if level == IntervalLevel.LEVEL_1DAY:
            return time_delta.days

        if level == IntervalLevel.LEVEL_1WEEK:
            return int(math.ceil(time_delta.days / 7))

        if level == IntervalLevel.LEVEL_1MON:
            return int(math.ceil(time_delta.days / 30))

        if time_delta.days > 0:
            seconds = (time_delta.days + 1) * one_day_trading_seconds
            return int(math.ceil(seconds / level.to_second()))
        else:
            seconds = time_delta.total_seconds()
            return min(int(math.ceil(seconds / level.to_second())),
                       one_day_trading_seconds / level.to_second())

    def eval_fetch_timestamps(self, entity, ref_record, http_session):
        latest_timestamp = None
        try:
            if pd_valid(ref_record):
                time_field = self.get_evaluated_time_field()
                latest_timestamp = ref_record[time_field].max(axis=0)
        except Exception as e:
            self.logger.warning(f'get ref record failed with error: {e}')

        if not latest_timestamp:
            latest_timestamp = entity.timestamp

        if not latest_timestamp:
            return self.start_timestamp, self.end_timestamp, self.default_size, None

        now = now_pd_timestamp(self.region)
        now_end = now.replace(hour=18, minute=0, second=0)

        trade_day_index = 0
        if len(self.trade_day) > 0:
            if is_same_date(self.trade_day[trade_day_index], now) and now < now_end:
                trade_day_index = 1
            end = self.trade_day[trade_day_index]
        else:
            end = now

        start_timestamp = next_dates(latest_timestamp)
        start = max(self.start_timestamp, start_timestamp) if self.start_timestamp else start_timestamp

        if start >= end:
            return start, end, 0, None

        size = self.eval_size_of_timestamp(start_timestamp=start,
                                           end_timestamp=end,
                                           level=self.level,
                                           one_day_trading_minutes=self.one_day_trading_minutes)

        return start, end, size, None


class TimestampsDataRecorder(TimeSeriesDataRecorder):

    def __init__(self,
                 entity_type: EntityType = EntityType.Stock,
                 exchanges=None,
                 entity_ids=None,
                 codes=None,
                 batch_size=10,
                 force_update=False,
                 sleeping_time=5,
                 default_size=findy_config['batch_size'],
                 real_time=False,
                 fix_duplicate_way='add',
                 start_timestamp=None,
                 end_timestamp=None,
                 close_hour=0,
                 close_minute=0,
                 share_para=None) -> None:
        super().__init__(entity_type, exchanges, entity_ids, codes, batch_size,
                         force_update, sleeping_time, default_size, real_time,
                         fix_duplicate_way, start_timestamp, end_timestamp,
                         close_hour=close_hour, close_minute=close_minute,
                         share_para=share_para)
        self.security_timestamps_map = {}

    def init_timestamps(self, entity_item, http_session) -> List[pd.Timestamp]:
        raise NotImplementedError

    def eval_fetch_timestamps(self, entity, ref_record, http_session):
        timestamps = self.security_timestamps_map.get(entity.id)
        if not timestamps:
            timestamps = self.init_timestamps(entity, http_session)
            if self.start_timestamp:
                timestamps = [t for t in timestamps if t >= self.start_timestamp]

            if self.end_timestamp:
                timestamps = [t for t in timestamps if t <= self.end_timestamp]

            self.security_timestamps_map[entity.id] = timestamps

        if not timestamps:
            return None, None, 0, timestamps

        timestamps.sort()

        latest_timestamp = None
        try:
            if pd_valid(ref_record):
                time_field = self.get_evaluated_time_field()
                latest_timestamp = ref_record[time_field].max(axis=0)
        except Exception as e:
            self.logger.warning(f'get ref_record failed with error: {e}')

        if latest_timestamp is not None and isinstance(latest_timestamp, pd.Timestamp):
            timestamps = [t for t in timestamps if t >= latest_timestamp]

            if timestamps:
                return timestamps[0], timestamps[-1], len(timestamps), timestamps
            return None, None, 0, None

        return timestamps[0], timestamps[-1], len(timestamps), timestamps


__all__ = ['Recorder', 'RecorderForEntities', 'KDataRecorder',
           'TimestampsDataRecorder', 'TimeSeriesDataRecorder']
