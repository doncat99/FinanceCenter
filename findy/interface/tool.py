# -*- coding: utf-8 -*-
import logging

from typing import List, Union

import pandas as pd

from sqlalchemy import func
from sqlalchemy.future import select

from findy.interface import Region, Provider, EntityType
from findy.database.schema.datatype import EntityMixin
from findy.database.schema.register import get_entity_schema_by_type

logger = logging.getLogger(__name__)


async def get_data_count(data_schema, db_session, filters=None):
    query = select(data_schema)
    if filters:
        for filter in filters:
            query = query.filter(filter)

    count_q = query.statement.with_only_columns([func.count()]).order_by(None)

    try:
        count = await db_session.execute(count_q).scalar()
        return count
    except Exception as e:
        logger.error(f"query {data_schema.__tablename__} failed with error: {e}")

    return None


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


async def get_entities(
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

    return await entity_schema.query_data(region=region, provider=provider, db_session=db_session,
                                          ids=ids, entity_ids=entity_ids, entity_id=entity_id,
                                          codes=codes, code=code, level=None, columns=columns,
                                          col_label=col_label, start_timestamp=start_timestamp,
                                          end_timestamp=end_timestamp, filters=filters,
                                          order=order, limit=limit, index=index)
