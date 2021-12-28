# -*- coding: utf-8 -*-
import logging
from typing import Union, List

import numpy as np
import pandas as pd

from sqlalchemy import func

from findy.interface import Region, Provider, EntityType
from findy.database.schema import ReportPeriod, IntervalLevel, AdjustType
from findy.database.schema.datatype import Mixin, EntityMixin, PortfolioStockHistory
from findy.database.schema.meta.fund_meta import Fund
from findy.database.schema.meta.stock_meta import Etf
from findy.database.schema.register import get_entity_schema_by_type
from findy.database.plugins.register import get_schema_by_name
from findy.database.context import get_db_session
from findy.utils.pd import pd_valid
from findy.utils.time import to_pd_timestamp

logger = logging.getLogger(__name__)


def decode_entity_id(entity_id: str):
    result = entity_id.split('_')
    entity_type = EntityType(result[0].lower())
    exchange = result[1]
    code = ''.join(result[2:])
    return entity_type, exchange, code


def get_entity_type(entity_id: str):
    entity_type, _, _ = decode_entity_id(entity_id)
    return entity_type


def get_entity_exchange(entity_id: str):
    _, exchange, _ = decode_entity_id(entity_id)
    return exchange


def get_entity_code(entity_id: str):
    _, _, code = decode_entity_id(entity_id)
    return code


def get_entities(
        region: Region,
        provider: Provider,
        db_session,
        entity_schema: EntityMixin = None,
        entity_type: EntityType = None,
        exchanges: List[str] = None,
        ids: List[str] = None,
        entity_ids: List[str] = None,
        entity_id: str = None,
        codes: List[str] = None,
        code: str = None,
        columns: List = None,
        col_label: dict = None,
        start_timestamp: Union[pd.Timestamp, str] = None,
        end_timestamp: Union[pd.Timestamp, str] = None,
        filters: List = None,
        order=None,
        limit: int = None,
        index: Union[str, list] = 'code') -> object:
    if not entity_schema:
        entity_schema = get_entity_schema_by_type(entity_type)

    # if not order:
    #     order = entity_schema.code.asc()

    if exchanges:
        if filters:
            filters.append(entity_schema.exchange.in_(exchanges))
        else:
            filters = [entity_schema.exchange.in_(exchanges)]

    return entity_schema.query_data(
        region=region, provider=provider, db_session=db_session,
        ids=ids, entity_ids=entity_ids, entity_id=entity_id,
        codes=codes, code=code, level=None, columns=columns,
        col_label=col_label, start_timestamp=start_timestamp,
        end_timestamp=end_timestamp, filters=filters,
        order=order, limit=limit, index=index)


def get_data_count(data_schema, db_session, filters=None):
    query = db_session.query(data_schema)
    if filters:
        for filter in filters:
            query = query.filter(filter)

    count_q = query.statement.with_only_columns([func.count()]).order_by(None)

    try:
        count = db_session.execute(count_q).scalar()
        return count
    except Exception as e:
        logger.error(f"query {data_schema.__tablename__} failed with error: {e}")

    return None


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


def get_recent_report_date(the_date, step=0):
    the_date = to_pd_timestamp(the_date)
    assert step >= 0
    if the_date.month >= 10:
        recent = f"{the_date.year}{'-09-30'}"
    elif the_date.month >= 7:
        recent = f"{the_date.year}{'-06-30'}"
    elif the_date.month >= 4:
        recent = f"{the_date.year}{'-03-31'}"
    else:
        recent = f"{the_date.year - 1}{'-12-31'}"

    if step == 0:
        return recent
    else:
        step = step - 1
        return get_recent_report_date(recent, step)


# def get_recent_report_period(the_date, step=0):
#     return to_report_period_type(get_recent_report_date(the_date, step=step))


def get_exchange(code):
    return 'sh' if code >= '333333' else 'sz'


def china_stock_code_to_id(code):
    return f"stock_{get_exchange(code)}_{code}"


def to_high_level_kdata(kdata_df: pd.DataFrame, to_level: IntervalLevel):
    def to_close(s):
        if pd_valid(s):
            return s[-1]

    def to_open(s):
        if pd_valid(s):
            return s[0]

    def to_high(s):
        return np.max(s)

    def to_low(s):
        return np.min(s)

    def to_sum(s):
        return np.sum(s)

    original_level = kdata_df['level'][0]
    entity_id = kdata_df['entity_id'][0]
    provider = kdata_df['provider'][0]
    name = kdata_df['name'][0]
    code = kdata_df['code'][0]

    entity_type, _, _ = decode_entity_id(entity_id=entity_id)

    assert IntervalLevel(original_level) <= IntervalLevel.LEVEL_1DAY
    assert IntervalLevel(original_level) < IntervalLevel(to_level)

    df: pd.DataFrame = None
    if to_level == IntervalLevel.LEVEL_1WEEK:
        # loffset='-2'　用周五作为时间标签
        if entity_type == EntityType.Stock:
            df = kdata_df.resample('W', loffset=pd.DateOffset(days=-2)).apply({'close': to_close,
                                                                               'open': to_open,
                                                                               'high': to_high,
                                                                               'low': to_low,
                                                                               'volume': to_sum,
                                                                               'turnover': to_sum})
        else:
            df = kdata_df.resample('W', loffset=pd.DateOffset(days=-2)).apply({'close': to_close,
                                                                               'open': to_open,
                                                                               'high': to_high,
                                                                               'low': to_low,
                                                                               'volume': to_sum,
                                                                               'turnover': to_sum})
    df = df.dropna()
    # id        entity_id  timestamp   provider    code  name level
    df['entity_id'] = entity_id
    df['provider'] = provider
    df['code'] = code
    df['name'] = name
    return df


def portfolio_relate_stock(df, portfolio):
    df['entity_id'] = portfolio.entity_id
    df['entity_type'] = portfolio.entity_type
    df['exchange'] = portfolio.exchange
    df['code'] = portfolio.code
    df['name'] = portfolio.name
    return df


# 季报只有前十大持仓，半年报和年报才有全量的持仓信息，故根据离timestamp最近的报表(年报 or 半年报)来确定持仓
async def get_portfolio_stocks(region: Region, provider: Provider, timestamp, portfolio_entity=Fund,
                               code=None, codes=None, ids=None):
    portfolio_stock = f'{portfolio_entity.__name__}Stock'
    data_schema: PortfolioStockHistory = get_schema_by_name(portfolio_stock)
    db_session = get_db_session(region, provider, data_schema)

    latests, column_names = data_schema.query_data(
        region=region,
        provider=provider,
        db_session=db_session,
        code=code,
        end_timestamp=timestamp,
        order=data_schema.timestamp.desc(),
        limit=1)

    if latests and len(latests) > 0:
        latest_record = latests[0]
        # 获取最新的报表
        data, column_names = data_schema.query_data(
            region=region,
            provider=provider,
            db_session=db_session,
            code=code,
            codes=codes,
            ids=ids,
            end_timestamp=timestamp,
            filters=[data_schema.report_date == latest_record.report_date])

        if data and len(data) > 0:
            df = pd.DataFrame([s.__dict__ for s in data], columns=column_names)

            # 最新的为年报或者半年报
            if latest_record.report_period == ReportPeriod.year or latest_record.report_period == ReportPeriod.half_year:
                return df
            # 季报，需要结合 年报或半年报 来算持仓
            else:
                step = 0
                while step <= 20:
                    report_date = get_recent_report_date(latest_record.report_date, step=step)

                    data, column_names = data_schema.query_data(
                        region=region,
                        provider=provider,
                        db_session=db_session,
                        code=code,
                        codes=codes,
                        ids=ids,
                        end_timestamp=timestamp,
                        filters=[data_schema.report_date == to_pd_timestamp(report_date)])

                    if data and len(data) > 0:
                        pre_df = pd.DataFrame.from_records([s.__dict__ for s in data], columns=column_names)
                        df = df.append(pre_df)

                    # 半年报和年报
                    if (ReportPeriod.half_year.value in pre_df['report_period'].tolist()) or (
                            ReportPeriod.year.value in pre_df['report_period'].tolist()):
                        # 保留最新的持仓
                        df = df.drop_duplicates(subset=['stock_code'], keep='first')
                        return df
                    step = step + 1


# etf半年报和年报才有全量的持仓信息，故根据离timestamp最近的报表(年报 or 半年报)来确定持仓
async def get_etf_stocks(region: Region, provider: Provider, timestamp, code=None, codes=None, ids=None):
    return await get_portfolio_stocks(region=region, provider=provider, timestamp=timestamp,
                                      portfolio_entity=Etf, code=code, codes=codes, ids=ids)


async def get_fund_stocks(region: Region, provider: Provider, timestamp, code=None, codes=None, ids=None):
    return await get_portfolio_stocks(region=region, provider=provider, timestamp=timestamp,
                                      portfolio_entity=Fund, code=code, codes=codes, ids=ids)


async def get_kdata(region: Region, entity_id=None, entity_ids=None,
                    level=IntervalLevel.LEVEL_1DAY.value, provider=None, columns=None,
                    start_timestamp=None, end_timestamp=None,
                    filters=None, db_session=None, order=None, limit=None,
                    index='timestamp', adjust_type: AdjustType = None):
    assert not entity_id or not entity_ids
    if entity_ids:
        entity_id = entity_ids[0]
    else:
        entity_ids = [entity_id]

    entity_type, exchange, code = decode_entity_id(entity_id)
    data_schema: Mixin = get_kdata_schema(entity_type, level=level, adjust_type=adjust_type)

    data, column_names = data_schema.query_data(
        region=region,
        provider=provider,
        db_session=db_session,
        entity_ids=entity_ids,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        level=level,
        columns=columns,
        filters=filters,
        order=order,
        limit=limit,
        index=index)

    if data and not columns:
        return pd.DataFrame([s.__dict__ for s in data], columns=column_names)

    return pd.DataFrame(data, columns=column_names)
