# -*- coding: utf-8 -*-
import logging
import time
from typing import List

import pandas as pd

from zvt import zvt_config
from zvt.api.data_type import Region, Provider, EntityType
from zvt.domain import StockTradeDay
from zvt.contract import IntervalLevel, Mixin, EntityMixin
from zvt.contract.api import df_to_db, get_entities, get_data
from zvt.utils.time_utils import to_pd_timestamp, eval_size_of_timestamp, is_same_date, now_pd_timestamp, next_date, to_time_str, PD_TIME_FORMAT_DAY
from zvt.utils.pd_utils import pd_is_not_null
from zvt.utils.mp_utils import run_amp, create_mp_share_value


class Meta(type):
    def __new__(meta, name, bases, class_dict):
        cls = type.__new__(meta, name, bases, class_dict)
        # register the recorder class to the data_schema
        if hasattr(cls, 'data_schema') and hasattr(cls, 'provider'):
            if cls.data_schema and issubclass(cls.data_schema, Mixin):
                # print(f'{cls.__name__}:{cls.data_schema.__name__}')
                assert cls.region in Region
                cls.data_schema.register_recorder_cls(cls.region,
                                                      cls.provider,
                                                      cls)
        return cls


class Recorder(metaclass=Meta):
    logger = logging.getLogger(__name__)

    # overwrite them to setup the data you want to record
    region: Region = None
    provider: Provider = Provider.Default
    data_schema: Mixin = None

    url = None

    def __init__(self,
                 batch_size: int = 10,
                 force_update: bool = False,
                 sleeping_time: int = 10) -> None:
        """
        :param batch_size:batch size to saving to db
        :type batch_size:int
        :param force_update: whether force update the data even if it exists,
                             please set it to True if the data need to
        be refreshed from the provider
        :type force_update:bool
        :param sleeping_time:sleeping seconds for recoding loop
        :type sleeping_time:int
        """
        self.logger = logging.getLogger(self.__class__.__name__)

        assert self.provider.value is not None
        assert self.data_schema is not None
        assert self.provider in self.data_schema.providers[self.region]

        self.batch_size = batch_size
        self.force_update = force_update
        self.sleeping_time = sleeping_time

    def run(self):
        raise NotImplementedError

    def sleep(self, sleeping_time=0):
        sleep_time = max(sleeping_time, self.sleeping_time)
        if sleep_time > 0:
            self.logger.info(f'sleeping {self.sleeping_time} seconds')
            time.sleep(sleep_time)


class RecorderForEntities(Recorder):
    # overwrite them to fetch the entity list
    provider: Provider = Provider.Default
    entity_schema: EntityMixin = None

    def __init__(self,
                 entity_type: EntityType = EntityType.Stock,
                 exchanges=['sh', 'sz'],
                 entity_ids=None,
                 codes=None,
                 batch_size=10,
                 force_update=False,
                 sleeping_time=10,
                 share_para=None) -> None:
        """
        :param entity_type:
        :type entity_type:
        :param exchanges:
        :type exchanges:
        :param entity_ids: set entity_ids or (entity_type,exchanges,codes)
        :type entity_ids:
        :param codes:
        :type codes:
        :param batch_size:
        :type batch_size:
        :param force_update:
        :type force_update:
        :param sleeping_time:
        :type sleeping_time:
        """
        super().__init__(batch_size=batch_size,
                         force_update=force_update,
                         sleeping_time=sleeping_time)

        assert self.provider.value is not None

        # setup the entities you want to record
        self.entity_type = entity_type
        self.exchanges = exchanges
        self.codes = codes
        self.share_para = share_para

        # set entity_ids or (entity_type,exchanges,codes)
        self.entity_ids = entity_ids

        if not hasattr(self, 'entities'):
            self.entities: List = None
            self.init_entities()

    def init_entities(self):
        """
        init the entities which we would record data for

        """
        assert self.region is not None

        # init the entity list
        self.entities = get_entities(region=self.region,
                                     entity_schema=self.entity_schema,
                                     entity_type=self.entity_type,
                                     exchanges=self.exchanges,
                                     entity_ids=self.entity_ids,
                                     codes=self.codes,
                                     return_type='domain',
                                     provider=self.provider)

    def process_loop(self, entity, http_session):
        raise NotImplementedError

    def run(self):
        if not hasattr(self, 'share_para') or self.share_para is None:
            self.share_para = (1, 'Total', 0)
        run_amp(self.share_para[2],
                self.share_para[0],
                self.process_loop,
                self.entities,
                self.share_para[1],
                create_mp_share_value())
        self.on_finish()


class TimeSeriesDataRecorder(RecorderForEntities):
    def __init__(self,
                 entity_type: EntityType = EntityType.Stock,
                 exchanges=['sh', 'sz'],
                 entity_ids=None,
                 codes=None,
                 batch_size=10,
                 force_update=False,
                 sleeping_time=5,
                 default_size=zvt_config['batch_size'],
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

    def get_referenced_saved_record(self, entity):
        return get_data(region=self.region,
                        entity_id=entity.id,
                        provider=self.provider,
                        data_schema=self.data_schema,
                        columns=['id', self.get_evaluated_time_field()],
                        return_type='df')

    def eval_fetch_timestamps(self, entity, ref_record, http_session):
        latest_timestamp = None
        try:
            if pd_is_not_null(ref_record):
                time_field = self.get_evaluated_time_field()
                latest_timestamp = ref_record[time_field].max(axis=0)
        except Exception as e:
            self.logger.warning(
                "get ref_record failed with error: {}".format(e))

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

    def record(self, entity, start, end, size, timestamps, http_session):
        """
        implement the recording logic in this method,
        should return json or domain list

        :param entity:
        :type entity:
        :param start:
        :type start:
        :param end:
        :type end:
        :param size:
        :type size:
        :param timestamps:
        :type timestamps:
        """
        raise NotImplementedError

    def format(self, entity, df):
        """
        implement the recording data formatting, should return dataframe

        :param entity:
        :type entity:
        :param df:
        :type df:
        """
        raise NotImplementedError

    def get_evaluated_time_field(self):
        """
        the timestamp field for evaluating time range of recorder,
        used in get_latest_saved_record

        """
        return 'timestamp'

    def get_original_time_field(self):
        return 'timestamp'

    def generate_domain_id(self, entity, df, time_fmt=PD_TIME_FORMAT_DAY):
        """
        generate domain id from the entity and original data,
        the default id meaning:entity + event happen time

        :param entity:
        :type entity:
        :param df:
        :type df:
        :param time_fmt:
        :type time_fmt:
        :return:
        :rtype:
        """
        time_field = self.get_evaluated_time_field()
        return entity.id + '_' + df[time_field].dt.strftime(time_fmt)

    def on_finish(self):
        pass

    def on_finish_entity(self, entity, http_session):
        pass

    def persist(self, ref_record, df_record, entity_item, http_session):
        saved_counts = 0
        entity_finished = False

        if pd_is_not_null(df_record):
            assert 'id' in df_record.columns
            saved_counts = df_to_db(df=df_record,
                                    ref_df=ref_record,
                                    region=self.region,
                                    data_schema=self.data_schema,
                                    provider=self.provider,
                                    fix_duplicate_way=self.fix_duplicate_way)
            if saved_counts == 0:
                entity_finished = True

        # could not get more data
        else:
            # not realtime
            if not self.real_time:
                entity_finished = True

            # realtime and to the close time
            elif (self.close_hour is not None) and (self.close_minute is not None):
                now = now_pd_timestamp(self.region)
                if now.hour >= self.close_hour:
                    if now.minute - self.close_minute >= 5:
                        self.logger.info(f'{entity_item.id} now is the close time: {now}')
                        entity_finished = True

        start_timestamp = to_time_str(df_record['timestamp'].min(axis=0))
        end_timestamp = to_time_str(df_record['timestamp'].max(axis=0))

        self.result = [saved_counts, start_timestamp, end_timestamp]

        # add finished entity to finished_items
        if entity_finished:
            self.on_finish_entity(entity_item, http_session)
        return entity_finished, saved_counts

    def process_entity(self, entity, http_session):
        step1 = time.time()
        precision_str = '{' + ':>{},.{}f'.format(8, 4) + '}'

        ref_record = self.get_referenced_saved_record(entity)

        cost = precision_str.format(time.time() - step1)
        self.logger.debug(f'get latest saved record: {len(ref_record)}, cost: {cost}')

        start, end, size, timestamps = \
            self.eval_fetch_timestamps(entity, ref_record, http_session)

        cost = precision_str.format(time.time() - step1)
        self.logger.debug(f'evaluate entity: {entity.id}, time: {cost}')

        # no more to record
        if size == 0:
            return True

        # fetch
        df_records = self.record(entity, start=start, end=end, size=size,
                                 timestamps=timestamps,
                                 http_session=http_session)

        cost = precision_str.format(time.time() - step1)
        record_cnt = 0 if df_records is None else len(df_records)
        self.logger.debug(f'record entity: {entity.id}, time: {cost}, size: {record_cnt}')

        if df_records is None:
            return True

        # format
        df_records = self.format(entity, df_records)
        cost = precision_str.format(time.time() - step1)
        self.logger.debug(f'format entity: {entity.id}, time: {cost}')

        # save
        is_finish, saved_cnt = self.persist(ref_record, df_records, entity, http_session)
        cost = precision_str.format(time.time() - step1)
        self.logger.debug(f'persist entity: {entity.id}, time: {cost}, with data count: {saved_cnt}')

        return is_finish

    def process_loop(self, entity, http_session):
        step1 = time.time()
        precision_str = '{' + ':>{},.{}f'.format(8, 4) + '}'

        self.result = None
        while not self.process_entity(entity, http_session):
            pass

        cost = precision_str.format(time.time() - step1)

        prefix = "finish~ " if zvt_config['debug'] else ""
        postfix = "\n" if zvt_config['debug'] else ""

        if self.result is not None:
            self.logger.info("{}{}, {:>18}, time: {}, size: {:>7}, date: [ {}, {} ]{}".format(
                prefix, self.data_schema.__name__, entity.id, cost,
                self.result[0], self.result[1], self.result[2], postfix))
        else:
            self.logger.info("{}{}, {:>18}, time: {}{}".format(
                prefix, self.data_schema.__name__, entity.id, cost, postfix))

    def run(self):
        trade_days = StockTradeDay.query_data(region=self.region,
                                              order=StockTradeDay.timestamp.desc(),
                                              return_type='domain')
        self.trade_day = [day.timestamp for day in trade_days]

        super().run()


class FixedCycleDataRecorder(TimeSeriesDataRecorder):
    def __init__(self,
                 entity_type: EntityType = EntityType.Stock,
                 exchanges=['sh', 'sz'],
                 entity_ids=None,
                 codes=None,
                 batch_size=10,
                 force_update=True,
                 sleeping_time=10,
                 default_size=zvt_config['batch_size'],
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
                 share_para=None) -> None:
        super().__init__(entity_type, exchanges, entity_ids, codes, batch_size,
                         force_update, sleeping_time, default_size, real_time,
                         fix_duplicate_way, start_timestamp, end_timestamp,
                         close_hour, close_minute, share_para=share_para)

        self.level = IntervalLevel(level)
        self.kdata_use_begin_time = kdata_use_begin_time
        self.one_day_trading_minutes = one_day_trading_minutes

    def eval_fetch_timestamps(self, entity, ref_record, http_session):
        latest_timestamp = None
        try:
            if pd_is_not_null(ref_record):
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
        if len(self.trade_day) > 0 and \
           is_same_date(self.trade_day[trade_day_index], now) and \
           now < now_end:
            trade_day_index = 1

        start_timestamp = next_date(latest_timestamp)
        start = max(self.start_timestamp, start_timestamp) if self.start_timestamp else start_timestamp

        if start >= self.trade_day[trade_day_index]:
            return start, None, 0, None

        size = eval_size_of_timestamp(start_timestamp=start,
                                      end_timestamp=self.trade_day[trade_day_index],
                                      level=self.level,
                                      one_day_trading_minutes=self.one_day_trading_minutes)

        return start, self.trade_day[trade_day_index], size, None


class TimestampsDataRecorder(TimeSeriesDataRecorder):

    def __init__(self,
                 entity_type: EntityType = EntityType.Stock,
                 exchanges=['sh', 'sz'],
                 entity_ids=None,
                 codes=None,
                 batch_size=10,
                 force_update=False,
                 sleeping_time=5,
                 default_size=zvt_config['batch_size'],
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
            if pd_is_not_null(ref_record):
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


__all__ = ['Recorder', 'RecorderForEntities', 'FixedCycleDataRecorder',
           'TimestampsDataRecorder', 'TimeSeriesDataRecorder']
