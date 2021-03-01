# -*- coding: utf-8 -*-
import logging
import time
from typing import List, Union, Type

import pandas as pd

from sqlalchemy import func
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import Query, Session, sessionmaker
from sqlalchemy_batch_inserts import enable_batch_inserting

from zvt import zvt_env, zvt_config
from zvt.api.data_type import Region, Provider, EntityType
from zvt.contract import zvt_context, IntervalLevel, EntityMixin, Mixin
from zvt.database.api import build_engine, to_postgresql, profiled
from zvt.utils.pd_utils import pd_is_not_null, index_df
from zvt.utils.time_utils import to_pd_timestamp

logger = logging.getLogger(__name__)


def get_db_name(data_schema: DeclarativeMeta) -> str:
    """
    get db name of the domain schema

    :param data_schema:
    :type data_schema:
    :return:
    :rtype:
    """
    for db_name, base in zvt_context.dbname_map_base.items():
        if issubclass(data_schema, base):
            return db_name


def get_db_engine(region: Region,
                  provider: Provider,
                  db_name: str = None,
                  data_schema: object = None,
                  data_path: str = zvt_env['data_path']) -> Engine:
    if data_schema:
        db_name = get_db_name(data_schema=data_schema)

    engine_key = '{}_{}_{}'.format(region.value, provider.value, db_name)
    db_engine = zvt_context.db_engine_map.get(engine_key)

    if not db_engine:
        # logger.info("engine key: {}".format(engine_key))
        region_key = '{}'.format(region.value)

        db_engine = zvt_context.db_region_map.get(region_key)
        if not db_engine:
            # logger.info("region key: {}".format(region_key))
            db_engine = build_engine(region)
            zvt_context.db_region_map[region_key] = db_engine

        zvt_context.db_engine_map[engine_key] = db_engine

    return db_engine


def get_db_session(region: Region,
                   provider: Provider,
                   db_name: str = None,
                   data_schema: object = None,
                   force_new: bool = False) -> Session:
    if data_schema:
        db_name = get_db_name(data_schema=data_schema)

    session_key = '{}_{}_{}'.format(region.value, provider.value, db_name)

    if force_new:
        return get_db_session_factory(region, provider, db_name, data_schema)()

    session = zvt_context.sessions.get(session_key)
    if not session:
        session = get_db_session_factory(region, provider, db_name, data_schema)()
        enable_batch_inserting(session)
        zvt_context.sessions[session_key] = session
    return session


def get_db_session_factory(region: Region,
                           provider: Provider,
                           db_name: str = None,
                           data_schema: object = None):
    if data_schema:
        db_name = get_db_name(data_schema=data_schema)

    session_key = '{}_{}_{}'.format(region.value, provider.value, db_name)
    session = zvt_context.db_session_map.get(session_key)
    if not session:
        session = sessionmaker()
        zvt_context.db_session_map[session_key] = session
    return session


def get_schema_by_name(name: str) -> DeclarativeMeta:
    for schema in zvt_context.schemas:
        if schema.__name__ == name:
            return schema


def get_schema_columns(schema: DeclarativeMeta) -> object:
    return schema.__table__.columns.keys()


def common_filter(query: Query,
                  data_schema,
                  start_timestamp=None,
                  end_timestamp=None,
                  filters=None,
                  order=None,
                  limit: int = None,
                  time_field='timestamp'):
    assert data_schema is not None
    time_col = eval('data_schema.{}'.format(time_field))

    if start_timestamp:
        query = query.filter(time_col >= to_pd_timestamp(start_timestamp))
    if end_timestamp:
        query = query.filter(time_col <= to_pd_timestamp(end_timestamp))

    if filters:
        for filter in filters:
            query = query.filter(filter)
    if order is not None:
        query = query.order_by(order)
    if limit:
        query = query.limit(limit)

    return query


def del_data(region: Region, data_schema: Type[Mixin], filters: List = None, provider: Provider = None):
    if not provider:
        provider = data_schema.providers[region][0]

    session = get_db_session(region=region, provider=provider, data_schema=data_schema)
    query = session.query(data_schema)
    if filters:
        for f in filters:
            query = query.filter(f)
    query.delete()
    session.commit()


def get_data(data_schema,
             region: Region,
             ids: List[str] = None,
             entity_ids: List[str] = None,
             entity_id: str = None,
             codes: List[str] = None,
             code: str = None,
             level: Union[IntervalLevel, str] = None,
             provider: Provider = Provider.Default,
             columns: List = None,
             col_label: dict = None,
             return_type: str = 'df',
             start_timestamp: Union[pd.Timestamp, str] = None,
             end_timestamp: Union[pd.Timestamp, str] = None,
             filters: List = None,
             session: Session = None,
             order=None,
             limit: int = None,
             index: Union[str, list] = None,
             time_field: str = 'timestamp'):
    assert data_schema is not None
    assert provider.value is not None
    assert provider in zvt_context.providers[region]

    step1 = time.time()
    precision_str = '{' + ':>{},.{}f'.format(8, 4) + '}'

    if not session:
        session = get_db_session(region=region, provider=provider, data_schema=data_schema)

    time_col = eval('data_schema.{}'.format(time_field))

    if columns:
        # support str
        if type(columns[0]) == str:
            columns_ = []
            for col in columns:
                assert isinstance(col, str)
                columns_.append(eval('data_schema.{}'.format(col)))
            columns = columns_

        # make sure get timestamp
        if time_col not in columns:
            columns.append(time_col)

        if col_label:
            columns_ = []
            for col in columns:
                if col.name in col_label:
                    columns_.append(col.label(col_label.get(col.name)))
                else:
                    columns_.append(col)
            columns = columns_

        query = session.query(*columns)
    else:
        query = session.query(data_schema)

    if zvt_config['debug'] == 2:
        cost = precision_str.format(time.time() - step1)
        logger.debug("get_data query column: {}".format(cost))

    if entity_id is not None:
        query = query.filter(data_schema.entity_id == entity_id)
    if entity_ids is not None:
        query = query.filter(data_schema.entity_id.in_(entity_ids))
    if code is not None:
        query = query.filter(data_schema.code == code)
    if codes is not None:
        query = query.filter(data_schema.code.in_(codes))
    if ids is not None:
        query = query.filter(data_schema.id.in_(ids))

    # we always store different level in different schema,the level param is not useful now
    # if level:
    #     try:
    #         # some schema has no level,just ignore it
    #         data_schema.level
    #         if type(level) == IntervalLevel:
    #             level = level.value
    #         query = query.filter(data_schema.level == level)
    #     except Exception as _:
    #         pass

    query = common_filter(query, data_schema=data_schema, start_timestamp=start_timestamp,
                          end_timestamp=end_timestamp, filters=filters, order=order, limit=limit,
                          time_field=time_field)

    if zvt_config['debug'] == 2:
        cost = precision_str.format(time.time() - step1)
        logger.debug("get_data query common: {}".format(cost))

    if return_type == 'df':
        df = pd.read_sql(query.statement, query.session.bind, index_col=['id'])
        if pd_is_not_null(df):
            if index:
                df = index_df(df, index=index, time_field=time_field)

        if zvt_config['debug'] == 2:
            cost = precision_str.format(time.time() - step1)
            logger.debug("get_data do query cost: {} type: {} size: {}".format(cost, return_type, len(df)))
        return df

    elif return_type == 'domain':
        # if limit is not None and limit == 1:
        #     result = [query.first()]
        # else:
        #     result = list(window_query(query, window_size, step1))
        # result = list(query.yield_per(window_size))

        if zvt_config['debug'] == 2:
            with profiled():
                result = query.all()
        else:
            result = query.all()

        if zvt_config['debug'] == 2:
            cost = precision_str.format(time.time() - step1)
            res_cnt = len(result) if result else 0
            logger.debug("get_data do query cost: {} type: {} limit: {} size: {}".format(cost, return_type, limit, res_cnt))

        return result

    elif return_type == 'dict':
        # if limit is not None and limit == 1:
        #     result = [item.__dict__ for item in query.first()]
        # else:
        #     result = [item.__dict__ for item in list(window_query(query, window_size, step1))]
        # result = [item.__dict__ for item in list(query.yield_per(window_size))]

        if zvt_config['debug'] == 2:
            with profiled():
                result = [item.__dict__ for item in query.all()]
        else:
            result = [item.__dict__ for item in query.all()]

        if zvt_config['debug'] == 2:
            cost = precision_str.format(time.time() - step1)
            res_cnt = len(result) if result else 0
            logger.debug("get_data do query cost: {} type: {} limit: {} size: {}".format(cost, return_type, limit, res_cnt))

        return result


# def window_query(query, window_size, timestamp):
#     start = 0
#     precision_str = '{' + ':>{},.{}f'.format(8, 4) + '}'

#     while True:
#         stop = start + window_size
#         things = query.slice(start, stop).all()
#         if len(things) == 0:
#             break
#         for thing in things:
#             yield thing
#         start += window_size
#         if zvt_config['debug']:
#             cost = precision_str.format(time.time()-timestamp)
#             logger.info("get_data do slice: {}".format(cost))


def get_group(region: Region, provider: Provider, data_schema, column, group_func=func.count, session=None):
    if not session:
        session = get_db_session(region=region, provider=provider, data_schema=data_schema)
    if group_func:
        query = session.query(column, group_func(column)).group_by(column)
    else:
        query = session.query(column).group_by(column)
    df = pd.read_sql(query.statement, query.session.bind)
    return df


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


def df_to_db(df: pd.DataFrame,
             ref_df: pd.DataFrame,
             region: Region,
             data_schema: DeclarativeMeta,
             provider: Provider,
             drop_duplicates: bool = True,
             fix_duplicate_way: str = 'ignore',
             force_update=False) -> object:
    step1 = time.time()
    precision_str = '{' + ':>{},.{}f'.format(8, 4) + '}'

    if not pd_is_not_null(df):
        return 0

    if drop_duplicates and df.duplicated(subset='id').any():
        df.drop_duplicates(subset='id', keep='last', inplace=True)

    schema_cols = get_schema_columns(data_schema)
    cols = list(set(df.columns.tolist()) & set(schema_cols))

    if not cols:
        logger.error("{} get columns failed".format(data_schema.__tablename__))
        return 0

    df = df[cols]

    # force update mode, delete duplicate id data in db, and write new data back
    if force_update:
        ids = df["id"].tolist()
        if len(ids) == 1:
            sql = f"delete from {data_schema.__tablename__} where id = '{ids[0]}'"
        else:
            sql = f"delete from {data_schema.__tablename__} where id in {tuple(ids)}"

        session = get_db_session(region=region, provider=provider, data_schema=data_schema)
        session.execute(sql)
        session.commit()
        df_new = df

    else:
        if ref_df is None:
            ref_df = get_data(region=region,
                              provider=provider,
                              columns=['id', 'timestamp'],
                              data_schema=data_schema,
                              return_type='df')
        if ref_df.empty:
            df_new = df
        else:
            df_new = df[~df.id.isin(ref_df.index)]

        # 不能单靠ID决定是否新增，要全量比对
        # if fix_duplicate_way == 'add':
        #     df_add = df[df.id.isin(ref_df.index)]
        #     if not df_add.empty:
        #         df_add.id = uuid.uuid1()
        #         df_new = pd.concat([df_new, df_add])

    cost = precision_str.format(time.time() - step1)
    logger.debug("remove duplicated: {}".format(cost))

    saved = 0
    if pd_is_not_null(df_new):
        saved = to_postgresql(region, df_new, data_schema.__tablename__)

    cost = precision_str.format(time.time() - step1)
    logger.debug("write db: {}, size: {}".format(cost, saved))

    return saved


def get_entities(
        region: Region,
        entity_schema: EntityMixin = None,
        entity_type: EntityType = None,
        exchanges: List[str] = None,
        ids: List[str] = None,
        entity_ids: List[str] = None,
        entity_id: str = None,
        codes: List[str] = None,
        code: str = None,
        provider: Provider = Provider.Default,
        columns: List = None,
        col_label: dict = None,
        return_type: str = 'df',
        start_timestamp: Union[pd.Timestamp, str] = None,
        end_timestamp: Union[pd.Timestamp, str] = None,
        filters: List = None,
        session: Session = None,
        order=None,
        limit: int = None,
        index: Union[str, list] = 'code') -> object:
    if not entity_schema:
        entity_schema = zvt_context.entity_schema_map[entity_type]

    if not provider.value:
        provider = entity_schema.providers[region][0]

    # if not order:
    #     order = entity_schema.code.asc()

    if exchanges:
        if filters:
            filters.append(entity_schema.exchange.in_(exchanges))
        else:
            filters = [entity_schema.exchange.in_(exchanges)]

    return get_data(data_schema=entity_schema, region=region, ids=ids, entity_ids=entity_ids, entity_id=entity_id, codes=codes,
                    code=code, level=None, provider=provider, columns=columns, col_label=col_label,
                    return_type=return_type, start_timestamp=start_timestamp, end_timestamp=end_timestamp,
                    filters=filters, session=session, order=order, limit=limit, index=index)

