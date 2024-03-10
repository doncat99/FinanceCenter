# -*- coding: utf-8 -*-
from typing import List, Union
import logging
import os
import time
import msgpack
import math
import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

import pandas as pd

from findy import findy_config
from findy.interface import Region, Provider, EntityType
from findy.database.schema import IntervalLevel, AdjustType
from findy.database.schema.datatype import Mixin, EntityMixin
from findy.database.schema.quotes.trade_day import StockTradeDay
from findy.database.schema.register import get_schema_by_name
from findy.database.context import get_db_session
from findy.database.quote import get_entities
from findy.utils.request import get_async_http_session
from findy.utils.kafka import connect_kafka_producer, publish_message
from findy.utils.progress import progress_topic, progress_key
from findy.utils.pd import pd_valid
from findy.utils.functool import time_it
from findy.utils.time import (PD_TIME_FORMAT_DAY, PRECISION_STR,
                              to_pd_timestamp, to_time_str,
                              now_pd_timestamp, next_date,
                              is_same_date)

kafka_producer = connect_kafka_producer(findy_config['kafka'])


class Meta(type):
    def __new__(meta, name, bases, class_dict):
        cls = type.__new__(meta, name, bases, class_dict)
        # register the recorder class to the data_schema
        if hasattr(cls, 'data_schema') and hasattr(cls, 'provider'):
            if cls.data_schema and issubclass(cls.data_schema, Mixin):
                # print(f'{cls.__name__}:{cls.data_schema.__name__}:{cls.region}:{cls.provider}')
                cls.data_schema.register_recorder_cls(cls.region, cls.provider, cls)
        # else:
        #     print(cls)
        return cls


class Recorder(metaclass=Meta):
    logger = logging.getLogger(__name__)

    def __init__(self,
                 batch_size: int = 10,
                 force_update: bool = False,
                 sleep_time: int = 10) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self.batch_size = batch_size
        self.force_update = force_update
        self.sleep_time = sleep_time

    async def run(self):
        raise NotImplementedError

    async def sleep(self, sleep_time=0.0):
        sleep_time = max(sleep_time, self.sleep_time)
        self.logger.debug(f'sleeping {sleep_time} seconds')
        return await asyncio.sleep(sleep_time)


class RecorderForEntities(Recorder):
    # overwrite them to setup the data you want to record
    region: Region = None
    provider: Provider = None
    data_schema: Mixin = None
    entity_schema: EntityMixin = None
    exchanges: List[str] = None

    def __init__(self,
                 entity_type: EntityType = EntityType.Stock,
                 entity_ids=None,
                 codes=None,
                 batch_size=10,
                 force_update=False,
                 sleep_time=10,
                 share_para=None) -> None:
        assert self.region is not None
        assert self.provider is not None
        assert self.data_schema is not None
        assert self.provider in self.data_schema.providers[self.region]

        # setup the entities you want to record
        self.entity_type = entity_type
        self.entity_ids = entity_ids
        self.codes = codes
        self.share_para = share_para

        super().__init__(batch_size=batch_size, force_update=force_update, sleep_time=sleep_time)

    async def init_entities(self, db_session):
        # init the entity list
        entities, column_names = get_entities(
            region=self.region,
            provider=self.provider,
            db_session=db_session,
            entity_schema=self.entity_schema,
            entity_type=self.entity_type,
            exchanges=self.exchanges,
            entity_ids=self.entity_ids,
            codes=self.codes)
        return entities

    async def eval(self, entity, http_session, db_session):
        raise NotImplementedError

    async def record(self, entity, http_session, db_session, para):
        raise NotImplementedError

    async def persist(self, entity, http_session, db_session, df_record):
        raise NotImplementedError

    async def on_finish_entity(self, entity, http_session, db_session, result):
        raise NotImplementedError

    async def on_finish(self, entities):
        raise NotImplementedError

    async def __process_entity(self, entity, http_session, db_session, concurrent):
        eval_time = 0
        download_time = 0
        persist_time = 0

        start_point = time.time()

        # eval
        eval_time, (is_finish, para) = await self.eval(entity, http_session, db_session)

        # data is up to date
        if is_finish:
            return 1, eval_time, download_time, persist_time, time.time() - start_point, None

        async with asyncio.Semaphore(concurrent):
            start_point = time.time()

            # fetch
            download_time, (is_finish, df_record) = await self.record(entity, http_session, db_session, para)
            if is_finish:
                # await self.sleep(0.1)
                return 2, eval_time, download_time, persist_time, time.time() - start_point + eval_time, None

            # save
            persist_time, (is_finish, extra) = await self.persist(entity, http_session, db_session, df_record)
            if is_finish:
                # await self.sleep(0.1)
                return 3, eval_time, download_time, persist_time, time.time() - start_point + eval_time, extra

        return 0, eval_time, download_time, persist_time, time.time() - start_point + eval_time, None

    async def process_loop(self, item):
        entity, pbar_update, concurrent = item

        http_session = get_async_http_session()
        db_session = get_db_session(self.region, self.provider, self.data_schema)

        eval_time = 0
        download_time = 0
        persist_time = 0
        total_time = 0

        while True:
            result, eval_, download_, persist_, total_, extra = await self.__process_entity(entity, http_session, db_session, concurrent)
            eval_time += eval_
            download_time += download_
            persist_time += persist_
            total_time += total_

            if result > 0:
                # add finished entity to finished_items
                time, _ = await self.on_finish_entity(entity, http_session, db_session, result)
                total_time += time
                break

        pbar_update["update"] = 1
        publish_message(kafka_producer, progress_topic, progress_key, msgpack.dumps(pbar_update))

        eval_time = PRECISION_STR.format(eval_time)
        download_time = PRECISION_STR.format(download_time)
        persist_time = PRECISION_STR.format(persist_time)
        total_time = PRECISION_STR.format(total_time)

        prefix = "finish~ " if findy_config['debug'] else ""
        postfix = "\n" if findy_config['debug'] else ""

        name = entity if isinstance(entity, str) else entity.id
        if extra is not None:
            if isinstance(extra, int):
                self.logger.info("{}{:>17}, {:>18}, eval: {}, download: {}, persist: {}, total: {}, size: {:>7}, {}".format(
                    prefix, self.data_schema.__name__, name, eval_time, download_time, persist_time, total_time,
                    extra, postfix))
            elif isinstance(extra, list):
                self.logger.info("{}{:>17}, {:>18}, eval: {}, download: {}, persist: {}, total: {}, size: {:>7}, date: [ {}, {} ]{}".format(
                    prefix, self.data_schema.__name__, name, eval_time, download_time, persist_time, total_time,
                    extra[0], extra[1], extra[2], postfix))
        else:
            self.logger.info("{}{:>17}, {:>18}, eval: {}, download: {}, persist: {}, total: {}{}".format(
                prefix, self.data_schema.__name__, name, eval_time, download_time, persist_time, total_time, postfix))

        await http_session.close()

    @staticmethod
    def async_to_sync(corofn, *args):
        loop = asyncio.new_event_loop()
        try:
            coro = corofn(*args)
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    async def run(self):
        db_session = get_db_session(self.region, self.provider, self.data_schema)
        entities = await self.init_entities(db_session)

        if entities and len(entities) > 0:
            taskid, processor, concurrent, desc = self.share_para[0:4]

            pbar_update = {"task": taskid, "total": len(entities), "desc": desc, "leave": True, "update": 0}
            publish_message(kafka_producer, progress_topic, progress_key, msgpack.dumps(pbar_update))

            items = [(entity, pbar_update, concurrent) for entity in entities]

            with ProcessPoolExecutor(max_workers=processor) as pool:
                loop = asyncio.get_event_loop()
                tasks = [loop.run_in_executor(pool, self.async_to_sync, self.process_loop, item) for item in items]

            # tasks = [asyncio.ensure_future(self.process_loop(item)) for item in items]
            [await result for result in asyncio.as_completed(tasks)]

            await self.on_finish(entities)


class TimeSeriesDataRecorder(RecorderForEntities):
    def __init__(self,
                 entity_type: EntityType = EntityType.Stock,
                 entity_ids=None,
                 codes=None,
                 batch_size=10,
                 force_update=False,
                 sleep_time=5,
                 fix_duplicate_way='add',
                 start_timestamp=None,
                 end_timestamp=None,
                 share_para=None) -> None:
        self.default_size = findy_config['batch_size']
        self.fix_duplicate_way = fix_duplicate_way
        self.start_timestamp = to_pd_timestamp(start_timestamp)
        self.end_timestamp = to_pd_timestamp(end_timestamp)

        super().__init__(entity_type, entity_ids, codes, batch_size,
                         force_update, sleep_time, share_para=share_para)

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

    async def eval_fetch_timestamps(self, entity, http_session, db_session):

        time_field = self.get_evaluated_time_field()
        try:
            time_column = eval(f'self.data_schema.{time_field}')
            latest_records, column_names = self.data_schema.query_data(
                region=self.region,
                provider=self.provider,
                db_session=db_session,
                entity_id=entity.entity_id,
                order=time_column.desc(),
                limit=1000)
            latest_timestamp = latest_records[0].timestamp if latest_records and len(latest_records) > 0 else None
        except Exception as e:
            self.logger.warning("get ref_record failed with error: {}".format(e))
            latest_timestamp = None

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

    @time_it
    async def eval(self, entity, http_session, db_session):
        # ref_record = await self.get_referenced_saved_record(entity, db_session)

        # cost = PRECISION_STR.format(time.time() - start_point)
        # self.logger.debug(f'get latest saved record: {len(ref_record)}, cost: {cost}')

        start, end, size, timestamps = \
            await self.eval_fetch_timestamps(entity, http_session, db_session)

        # no more to record
        is_finish = True if size == 0 else False

        return is_finish, (start, end, size, timestamps)

    @time_it
    async def persist(self, entity, http_session, db_session, df_record):
        saved_counts = 0
        is_finished = False

        if pd_valid(df_record):
            assert 'id' in df_record.columns
            from findy.database.persist import df_to_db

            saved_counts = await df_to_db(region=self.region,
                                          provider=self.provider,
                                          data_schema=self.data_schema,
                                          db_session=db_session,
                                          df=df_record,
                                          ref_entity=entity,
                                          fix_duplicate_way=self.fix_duplicate_way)
            if saved_counts == 0:
                is_finished = True

        # could not get more data
        else:
            is_finished = True

        if isinstance(self, KDataRecorder):
            is_finished = True

        start_timestamp = to_time_str(df_record['timestamp'].min(axis=0))
        end_timestamp = to_time_str(df_record['timestamp'].max(axis=0))

        return is_finished, [saved_counts, start_timestamp, end_timestamp]

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
                 entity_ids=None,
                 codes=None,
                 batch_size=10,
                 force_update=True,
                 sleep_time=10,
                 fix_duplicate_way='ignore',
                 start_timestamp=None,
                 end_timestamp=None,
                 # child add
                 level=IntervalLevel.LEVEL_1DAY,
                 share_para=None):
        super().__init__(entity_type, entity_ids, codes, batch_size,
                         force_update, sleep_time,
                         fix_duplicate_way, start_timestamp, end_timestamp,
                         share_para=share_para)
        self.level = IntervalLevel(level)
        self.default_size = findy_config['batch_size']

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
            return round(time_delta.days / 7)

        if level == IntervalLevel.LEVEL_1MON:
            return round(time_delta.days / 31)

        if time_delta.days > 0:
            seconds = (time_delta.days + 1) * one_day_trading_seconds
            return int(math.ceil(seconds / level.to_second()))
        else:
            seconds = time_delta.total_seconds()
            return min(int(math.ceil(seconds / level.to_second())),
                       one_day_trading_seconds / level.to_second())

    async def eval_fetch_timestamps(self, entity, http_session, db_session):
        time_field = self.get_evaluated_time_field()
        try:
            time_column = eval(f'self.data_schema.{time_field}')
            latest_records, column_names = self.data_schema.query_data(
                region=self.region,
                provider=self.provider,
                db_session=db_session,
                entity_id=entity.entity_id,
                order=time_column.desc(),
                limit=1000)
            latest_timestamp = latest_records[0].timestamp if latest_records and len(latest_records) > 0 else None
        except Exception as e:
            self.logger.warning(f'get ref record failed with error: {e}')
            latest_timestamp = None

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

        try:
            start_timestamp = next_date(latest_timestamp)
        except Exception as e:
            print('next_date exception:', e)

        start = max(self.start_timestamp, start_timestamp) if self.start_timestamp else start_timestamp

        if start >= end:
            return start, end, 0, None

        size = self.eval_size_of_timestamp(start_timestamp=start,
                                           end_timestamp=end,
                                           level=self.level,
                                           one_day_trading_minutes=4 * 60)

        return start, end, size, None


class TimestampsDataRecorder(TimeSeriesDataRecorder):

    def __init__(self,
                 entity_type: EntityType = EntityType.Stock,
                 entity_ids=None,
                 codes=None,
                 batch_size=10,
                 force_update=False,
                 sleep_time=5,
                 fix_duplicate_way='add',
                 start_timestamp=None,
                 end_timestamp=None,
                 share_para=None) -> None:
        super().__init__(entity_type, entity_ids, codes, batch_size,
                         force_update, sleep_time,
                         fix_duplicate_way, start_timestamp, end_timestamp,
                         share_para=share_para)
        self.security_timestamps_map = {}

    def init_timestamps(self, entity_item, http_session) -> List[pd.Timestamp]:
        raise NotImplementedError

    async def eval_fetch_timestamps(self, entity, http_session, db_session):
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

        time_field = self.get_evaluated_time_field()
        try:
            time_column = eval(f'self.data_schema.{time_field}')
            latest_records, column_names = self.data_schema.query_data(
                region=self.region,
                provider=self.provider,
                db_session=db_session,
                entity_id=entity.entity_id,
                order=time_column.desc(),
                limit=1000)
            latest_timestamp = latest_records[0].timestamp if latest_records and len(latest_records) > 0 else None
        except Exception as e:
            self.logger.warning(f'get ref_record failed with error: {e}')
            latest_timestamp = None

        if latest_timestamp is not None and isinstance(latest_timestamp, pd.Timestamp):
            timestamps = [t for t in timestamps if t >= latest_timestamp]

            if timestamps:
                return timestamps[0], timestamps[-1], len(timestamps), timestamps
            return None, None, 0, None

        return timestamps[0], timestamps[-1], len(timestamps), timestamps


__all__ = ['Recorder', 'RecorderForEntities', 'KDataRecorder',
           'TimestampsDataRecorder', 'TimeSeriesDataRecorder']
