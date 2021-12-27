import logging
import time
from typing import List, Type

import pandas as pd

from sqlalchemy.ext.declarative import DeclarativeMeta

from findy.interface import Region, Provider
from findy.database.schema.datatype import Mixin
from findy.database.plugins.register import get_schema_columns
from findy.database.persist import to_postgresql
from findy.utils.pd import pd_valid
from findy.utils.time import PRECISION_STR

logger = logging.getLogger(__name__)


async def df_to_db(region: Region,
                   provider: Provider,
                   data_schema: DeclarativeMeta,
                   db_session,
                   df: pd.DataFrame,
                   ref_df: pd.DataFrame = None,
                   drop_duplicates: bool = True,
                   fix_duplicate_way: str = 'ignore',
                   force_update=False) -> object:
    now = time.time()

    if not pd_valid(df):
        return 0

    if drop_duplicates and df.duplicated(subset='id').any():
        df.drop_duplicates(subset='id', keep='last', inplace=True)

    schema_cols = get_schema_columns(data_schema)
    cols = list(set(df.columns.tolist()) & set(schema_cols))

    if not cols:
        logger.error(f"{data_schema.__tablename__} get columns failed")
        return 0

    df = df[cols]

    # force update mode, delete duplicate id data, and rewrite new data back
    if force_update:
        ids = df["id"].tolist()
        if len(ids) == 1:
            sql = f"delete from {data_schema.__tablename__} where id = '{ids[0]}'"
        else:
            sql = f"delete from {data_schema.__tablename__} where id in {tuple(ids)}"

        try:
            db_session.execute(sql)
            db_session.commit()
        except Exception as e:
            logger.error(f"query {data_schema.__tablename__} failed with error: {e}")

        df_new = df

    else:
        if ref_df is None:
            data, column_names = data_schema.query_data(
                region=region,
                provider=provider,
                db_session=db_session,
                columns=[data_schema.id, data_schema.timestamp])

            if data and len(data) > 0:
                ref_df = pd.DataFrame(data, columns=column_names)

        if pd_valid(ref_df):
            df_new = df[~df.id.isin(ref_df.id)]
        else:
            df_new = df

        # 不能单靠ID决定是否新增，要全量比对
        # if fix_duplicate_way == 'add':
        #     df_add = df[df.id.isin(ref_df.id)]
        #     if not df_add.empty:
        #         df_add.id = uuid.uuid1()
        #         df_new = pd.concat([df_new, df_add])

    cost = PRECISION_STR.format(time.time() - now)
    logger.debug(f"remove duplicated: {cost}")

    saved = 0
    if pd_valid(df_new):
        saved = to_postgresql(region, df_new, data_schema.__tablename__)

    cost = PRECISION_STR.format(time.time() - now)
    logger.debug(f"write db: {cost}, size: {saved}")

    return saved


def del_data(db_session, data_schema: Type[Mixin], filters: List = None):
    query = db_session.query(data_schema)
    if filters:
        for f in filters:
            query = query.filter(f)
    query.delete()

    try:
        db_session.execute(query)
        db_session.commit()
    except Exception as e:
        logger.error(f"query {data_schema.__tablename__} failed with error: {e}")
