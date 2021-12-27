# -*- coding: utf-8 -*-
import inspect
from datetime import timedelta
from typing import List, Union

import pandas as pd
from sqlalchemy import Column, Float, String, Boolean, DateTime

from findy.interface import Region, Provider
from findy.database.schema import IntervalLevel
from findy.utils.time import to_pd_datetime, is_same_time, now_pd_timestamp


class Mixin(object):
    id = Column(String, primary_key=True)
    # entity id for this model
    entity_id = Column(String)

    # the meaning could be different for different case,most of time it means 'happen time'
    timestamp = Column(DateTime)

    # __mapper_args__ = {
    #    "order_by": timestamp.desc()
    # }

    @classmethod
    def help(cls):
        print(inspect.getsource(cls))

    @classmethod
    def important_cols(cls):
        return []

    @classmethod
    def time_field(cls):
        return 'timestamp'

    @classmethod
    def register_recorder_cls(cls, region: Region, provider: Provider, recorder_cls):
        """
        register the recorder for the schema

        :param provider:
        :param recorder_cls:
        """
        # don't make provider_map_recorder as class field,it should be created for the sub class as need
        if not hasattr(cls, 'provider_map_recorder'):
            cls.provider_map_recorder = {}

        if region not in cls.provider_map_recorder:
            cls.provider_map_recorder[region] = {}
            cls.provider_map_recorder[region][provider] = recorder_cls

        elif provider not in cls.provider_map_recorder[region]:
            cls.provider_map_recorder[region][provider] = recorder_cls

    @classmethod
    def register_provider(cls, region: Region, provider: Provider):
        # dont't make providers as class field,it should be created for the sub class as need
        if not hasattr(cls, 'providers'):
            cls.providers = {}

        if region in cls.providers.keys():
            if provider not in cls.providers[region]:
                cls.providers[region].append(provider)
        else:
            cls.providers.update({region: [provider]})

    @classmethod
    def query_data(
            cls,
            region: Region,
            provider: Provider,
            db_session,
            ids: List[str] = None,
            entity_ids: List[str] = None,
            entity_id: str = None,
            codes: List[str] = None,
            code: str = None,
            level: Union[IntervalLevel, str] = None,
            columns: List = None,
            col_label: dict = None,
            start_timestamp: Union[pd.Timestamp, str] = None,
            end_timestamp: Union[pd.Timestamp, str] = None,
            filters: List = None,
            order=None,
            limit: int = None,
            index: Union[str, list] = None,
            time_field: str = 'timestamp',
            func=None):
        from findy.database.query import get_data
        return get_data(
            region=region, provider=provider, data_schema=cls, db_session=db_session,
            ids=ids, entity_ids=entity_ids, entity_id=entity_id, codes=codes,
            code=code, level=level, columns=columns, col_label=col_label,
            start_timestamp=start_timestamp, end_timestamp=end_timestamp,
            filters=filters, order=order, limit=limit, index=index,
            time_field=time_field, fun=func)

    @classmethod
    async def record_data(cls,
                          region: Region,
                          provider: Provider,
                          exchanges=None,
                          entity_ids=None,
                          codes=None,
                          batch_size=None,
                          force_update=None,
                          sleeping_time=None,
                          default_size=None,
                          real_time=None,
                          fix_duplicate_way=None,
                          start_timestamp=None,
                          end_timestamp=None,
                          close_hour=None,
                          close_minute=None,
                          one_day_trading_minutes=None,
                          **kwargs):
        assert hasattr(cls, 'provider_map_recorder') and cls.provider_map_recorder
        # print(f'{cls.__name__} registered recorders:{cls.provider_map_recorder}')

        assert region is not None or provider is not None

        recorder_class = cls.provider_map_recorder[region][provider]

        # get args for specific recorder class
        from findy.database.plugins.recorder import TimeSeriesDataRecorder
        if issubclass(recorder_class, TimeSeriesDataRecorder):
            args = [item for item in inspect.getfullargspec(cls.record_data).args if
                    item not in ('cls', 'region', 'provider')]
        else:
            args = ['batch_size', 'force_update', 'sleeping_time']

        # just fill the None arg to kw,so we could use the recorder_class default args
        kw = {}
        for arg in args:
            tmp = eval(arg)
            if tmp is not None:
                kw[arg] = tmp

        # KDataRecorder
        from findy.database.plugins.recorder import KDataRecorder
        if issubclass(recorder_class, KDataRecorder):
            # contract:
            # 1)use KDataRecorder to record the data with IntervalLevel
            # 2)the table of schema with IntervalLevel format is {entity}_{level}_[adjust_type]_{event}
            table: str = cls.__tablename__
            try:
                items = table.split('_')
                if len(items) == 4:
                    adjust_type = items[2]
                    kw['adjust_type'] = adjust_type
                level = IntervalLevel(items[1])
            except:
                # for other schema not with normal format,but need to calculate size for remaining days
                level = IntervalLevel.LEVEL_1DAY

            kw['level'] = level

        # add other custom args
        for k in kwargs:
            kw[k] = kwargs[k]

        r = recorder_class(**kw)
        await r.run()


class NormalMixin(Mixin):
    # the record created time in db
    created_timestamp = Column(DateTime, default=now_pd_timestamp(Region.CHN))
    # the record updated time in db, some recorder would check it for whether need to refresh
    updated_timestamp = Column(DateTime)


class EntityMixin(Mixin):
    # 标的类型
    entity_type = Column(String(length=64))
    # 所属交易所
    exchange = Column(String(length=32))
    # 编码
    code = Column(String(length=64))
    # 名字
    name = Column(String(length=256))
    # 上市日
    list_date = Column(DateTime)
    # 退市日
    end_date = Column(DateTime)
    # 交易状态
    is_active = Column(Boolean, default=True)

    @classmethod
    def get_trading_dates(cls, start_date=None, end_date=None):
        """
        overwrite it to get the trading dates of the entity

        :param start_date:
        :param end_date:
        :return:
        """
        return pd.date_range(start_date, end_date, freq='B')

    @classmethod
    def get_trading_intervals(cls):
        """
        overwrite it to get the trading intervals of the entity

        :return:[(start,end)]
        """
        return [('09:30', '11:30'), ('13:00', '15:00')]

    @classmethod
    def get_interval_timestamps(cls, start_date, end_date, level: IntervalLevel):
        """
        generate the timestamps for the level

        :param start_date:
        :param end_date:
        :param level:
        """

        for current_date in cls.get_trading_dates(start_date=start_date, end_date=end_date):
            if level >= IntervalLevel.LEVEL_1DAY:
                yield current_date
            else:
                start_end_list = cls.get_trading_intervals()

                for start_end in start_end_list:
                    start = start_end[0]
                    end = start_end[1]

                    current_timestamp = to_pd_datetime(the_date=current_date, the_time=start)
                    end_timestamp = to_pd_datetime(the_date=current_date, the_time=end)

                    while current_timestamp <= end_timestamp:
                        yield current_timestamp
                        current_timestamp = current_timestamp + timedelta(minutes=level.to_minute())

    @classmethod
    def is_open_timestamp(cls, timestamp):
        timestamp = pd.Timestamp(timestamp)
        return is_same_time(timestamp, to_pd_datetime(the_date=timestamp.date(),
                                                      the_time=cls.get_trading_intervals()[0][0]))

    @classmethod
    def is_close_timestamp(cls, timestamp):
        timestamp = pd.Timestamp(timestamp)
        return is_same_time(timestamp, to_pd_datetime(the_date=timestamp.date(),
                                                      the_time=cls.get_trading_intervals()[-1][1]))

    @classmethod
    def is_finished_kdata_timestamp(cls, timestamp: pd.Timestamp, level: IntervalLevel):

        timestamp = pd.Timestamp(timestamp)

        for t in cls.get_interval_timestamps(timestamp.date(), timestamp.date(), level=level):
            if is_same_time(t, timestamp):
                return True

        return False

    @classmethod
    def could_short(cls):
        """
        whether could be shorted

        :return:
        """
        return False

    @classmethod
    def get_trading_t(cls):
        """
        0 means t+0
        1 means t+1

        :return:
        """
        return 1


class NormalEntityMixin(EntityMixin):
    # the record created time in db
    created_timestamp = Column(DateTime, default=now_pd_timestamp(Region.CHN))
    # the record updated time in db, some recorder would check it for whether need to refresh
    updated_timestamp = Column(DateTime)


class Portfolio(EntityMixin):
    @classmethod
    async def get_stocks(cls, region: Region, provider: Provider, timestamp, code=None, codes=None, ids=None):
        """
        the publishing policy of portfolio positions is different for different types,
        overwrite this function for get the holding stocks in specific date

        :param code: portfolio(etf/block/index...) code
        :param codes: portfolio(etf/block/index...) codes
        :param ids: portfolio(etf/block/index...) ids
        :param timestamp: the date of the holding stocks
        :param provider: the data provider
        :return:
        """
        from findy.database.plugins.register import get_schema_by_name
        from findy.database.context import get_db_session

        schema_str = f'{cls.__name__}Stock'
        portfolio_stock = get_schema_by_name(schema_str)
        db_session = get_db_session(region, provider, data_schema=portfolio_stock)
        data, column_names = portfolio_stock.query_data(
            region=region,
            provider=provider,
            db_session=db_session,
            code=code,
            codes=codes,
            timestamp=timestamp,
            ids=ids)

        if data and len(data) > 0:
            return pd.DataFrame([s.__dict__ for s in data], columns=column_names)
        else:
            return pd.DataFrame()


# 组合(Fund,Etf,Index,Block等)和个股(Stock)的关系 应该继承自该类
# 该基础类可以这样理解:
# entity为组合本身,其包含了stock这种entity,timestamp为持仓日期,从py的"你知道你在干啥"的哲学出发，不加任何约束
class PortfolioStock(Mixin):
    # portfolio标的类型
    entity_type = Column(String(length=64))
    # portfolio所属交易所
    exchange = Column(String(length=32))
    # portfolio编码
    code = Column(String(length=64))
    # portfolio名字
    name = Column(String(length=128))

    stock_id = Column(String)
    stock_code = Column(String(length=64))
    stock_name = Column(String(length=256))


# 支持时间变化,报告期标的调整
class PortfolioStockHistory(PortfolioStock):
    # 报告期,season1,half_year,season3,year
    report_period = Column(String(length=32))
    # 3-31,6-30,9-30,12-31
    report_date = Column(DateTime)

    # 占净值比例
    proportion = Column(Float)
    # 持有股票的数量
    shares = Column(Float)
    # 持有股票的市值
    market_cap = Column(Float)


class KdataCommon(Mixin):
    provider = Column(String(length=32))
    code = Column(String(length=32))
    name = Column(String(length=256))
    # Enum constraint is not extendable
    # level = Column(Enum(IntervalLevel, values_callable=enum_value))
    level = Column(String(length=32))

    # 如果是股票，代表前复权数据
    # 开盘价
    open = Column(Float)
    # 收盘价
    close = Column(Float)
    # 最高价
    high = Column(Float)
    # 最低价
    low = Column(Float)
    # 成交量
    volume = Column(Float)
    # 成交金额
    turnover = Column(Float)


class TickCommon(Mixin):
    provider = Column(String(length=32))
    code = Column(String(length=32))
    name = Column(String(length=256))
    level = Column(String(length=32))

    order = Column(String(length=32))
    price = Column(Float)
    volume = Column(Float)
    turnover = Column(Float)
    direction = Column(String(length=32))
    order_type = Column(String(length=32))


class BlockKdataCommon(KdataCommon):
    pass


class IndexKdataCommon(KdataCommon):
    pass


class EtfKdataCommon(KdataCommon):
    turnover_rate = Column(Float)

    # ETF 累计净值（货币 ETF 为七日年化)
    cumulative_net_value = Column(Float)
    # ETF 净值增长率
    change_pct = Column(Float)


class StockKdataCommon(KdataCommon):
    # 涨跌幅
    change_pct = Column(Float)
    # 换手率
    turnover_rate = Column(Float)
