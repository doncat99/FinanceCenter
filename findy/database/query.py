# -*- coding: utf-8 -*-
import logging
from typing import List, Union
# import time
import pandas as pd

from sqlalchemy.orm import Query

# from findy import findy_config
from findy.interface import Region, Provider
from findy.database.schema import IntervalLevel
from findy.database.plugins.register import providers
# from findy.database.context import  profiled
# from findy.database.persist import from_postgresql
from findy.utils.time import PRECISION_STR, to_pd_timestamp
# from findy.utils.pd import pd_valid, index_df

logger = logging.getLogger(__name__)


def common_filter(query: Query,
                  data_schema,
                  ids: List[str] = None,
                  entity_ids: List[str] = None,
                  entity_id: str = None,
                  codes: List[str] = None,
                  code: str = None,
                  start_timestamp=None,
                  end_timestamp=None,
                  filters=None,
                  order=None,
                  limit: int = None,
                  time_field='timestamp'):
    assert data_schema is not None

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

    time_col = eval(f'data_schema.{time_field}')

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


def column_query(data_schema, db_session, columns, time_field, col_label):
    # support str
    if type(columns[0]) == str:
        columns_ = []
        for col in columns:
            assert isinstance(col, str)
            columns_.append(eval(f'data_schema.{col}'))
        columns = columns_

    # make sure get timestamp
    time_col = eval(f'data_schema.{time_field}')
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

    return db_session.query(*columns)


def get_data(
        region: Region,
        provider: Provider,
        data_schema,
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
        fun=None):
    assert data_schema is not None
    assert db_session is not None
    assert provider is not None
    assert provider in providers[region]

    # now = time.time()

    if columns:
        query = column_query(data_schema, db_session, columns, time_field, col_label)
    elif fun is not None:
        query = db_session.query(fun)
    else:
        query = db_session.query(data_schema)

    # if findy_config['debug'] == 2:
    #     cost = PRECISION_STR.format(time.time() - now)
    #     logger.debug(f"get_data query column: {cost}")

    query = common_filter(query,
                          data_schema=data_schema,
                          ids=ids,
                          entity_ids=entity_ids,
                          entity_id=entity_id,
                          codes=codes,
                          code=code,
                          start_timestamp=start_timestamp,
                          end_timestamp=end_timestamp,
                          filters=filters,
                          order=order,
                          limit=limit,
                          time_field=time_field)
    # if not db_session:
    #     db_session = get_db_session(region, provider, data_schema)

    try:
        result = db_session.execute(query)
        result_columns = query.statement.columns.keys()
    except Exception as e:
        logger.error(f"query {data_schema.__tablename__} failed with error: {e}")
        return None, []

    if columns:
        result = result.all()
    elif fun is not None:
        result = result.scalar()
    else:
        result = result.scalars().all()

    # if findy_config['debug'] == 2:
    #     cost = PRECISION_STR.format(time.time() - now)
    #     logger.debug(f"get_data query common: {cost}")

    # if fun is not None:
    #     # result = query.scalar()
    #     result = db_session.execute(query).scalars().all()
    #     return (result, query.statement.columns.keys())

    # if return_type == 'df':
    #     df = from_postgresql(region, query.statement)
    #     if pd_valid(df) and index:
    #         df = index_df(df, index=index, time_field=time_field)
    #     else:
    #         df = pd.DataFrame()
    #     return df

    # if return_type == 'df':
    #     df = pd.read_sql(query.statement, query.session.bind)
    #     if pd_valid(df) and index:
    #         df = index_df(df, index=index, time_field=time_field)
    #     return df

    # if findy_config['debug'] == 2:
    #     with profiled():
    #         # result = query.all()
    #         result = db_session.execute(query).all()
    # else:
        # result = query.all()

    # if findy_config['debug'] == 2:
    #     cost = PRECISION_STR.format(time.time() - now)
    #     res_cnt = len(result) if result else 0
    #     logger.debug(f"get_data do query cost: {cost}  limit: {limit} size: {res_cnt}")

    # if return_type == 'df':
    #     if len(result) > 0:
    #         df = pd.DataFrame(result, columns=result[0].keys())
    #         df.set_index('id', drop=True, inplace=True)
    #     else:
    #         df = pd.DataFrame()

    #     if pd_valid(df) and index:
    #         df = index_df(df, index=index, time_field=time_field)
    #     return df

    return (result, result_columns)
