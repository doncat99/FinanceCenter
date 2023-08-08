import logging
import os
import time
from io import StringIO

import pandas as pd
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.declarative import DeclarativeMeta

from findy import findy_env
from findy.interface import Region, Provider
from findy.database.schema.register import get_schema_columns
from findy.database.context import get_db_engine
from findy.utils.pd import pd_valid
from findy.utils.time import PRECISION_STR

logger = logging.getLogger(__name__)


async def df_to_db(region: Region,
                   provider: Provider,
                   data_schema: DeclarativeMeta,
                   db_session,
                   df: pd.DataFrame,
                   ref_entity = None,
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
        except Exception as e:
            logger.error(f"query {data_schema.__tablename__} failed with error: {e}")

        try:
            db_session.commit()
        except Exception as e:
            logger.error(f'df_to_db {data_schema.__tablename__}, error: {e}')
            db_session.rollback()
        finally:
            db_session.close()
        df_new = df

    else:
        ref_df = None
        if ref_entity is not None:
            data, column_names = data_schema.query_data(
                region=region,
                provider=provider,
                db_session=db_session,
                entity_id=ref_entity.id,
                columns=[data_schema.id, data_schema.timestamp])
                # order=data_schema.desc(),
                # limit=1000)
        else:
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

    rmdup = time.time()
    cost = PRECISION_STR.format(rmdup - now)
    logger.debug(f"remove duplicated: {cost}")

    saved = 0
    if pd_valid(df_new):
        saved = to_postgresql(region, df_new, data_schema.__tablename__)

    cost = PRECISION_STR.format(time.time() - rmdup)
    logger.debug(f"write db: {cost}, size: {saved}")

    return saved


def to_postgresql(region: Region, df, tablename):
    # now = time.time()

    saved = len(df)
    output = StringIO()
    df.to_csv(output, sep='\t', index=False, header=False, encoding='utf-8')
    output.seek(0)

    # to_csv_time = time.time()
    # cost = PRECISION_STR.format(to_csv_time - now)
    # logger.debug(f"dataFrame to IO: {cost}, size: {saved}")

    db_engine = get_db_engine(region)
    connection = db_engine.raw_connection()
    cursor = connection.cursor()
    
    try:
        cursor.copy_from(output, tablename, null='', size=1024 * 16, columns=list(df.columns))
    except Exception as e:
        logger.error(f'copy_from failed on table: [ {tablename} ], {e}')
        err_msg = str(e).replace("\"", "")
        df.to_csv(os.path.join(findy_env['err_path'], f'{err_msg[:min(len(err_msg), 50)]}.csv'))
        cursor.close()
        connection.close()
        return 0
    
    try:
        connection.commit()
    except Exception as e:
        logger.error(f'copy_from commit failed on table: [ {tablename} ], {e}')
        connection.rollback()
    finally:
        cursor.close()
        connection.close()
    
    # to_db_time = time.time()
    # cost = PRECISION_STR.format(to_db_time - to_csv_time)
    # logger.debug(f"write to db: {cost}, size: {saved}")

    return saved


def from_postgresql(region: Region, query):
    statement = query.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})
    copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(query=statement, head="HEADER")

    db_engine = get_db_engine(region)
    connection = db_engine.raw_connection()
    cursor = connection.cursor()

    try:
        store = StringIO()
        cursor.copy_expert(copy_sql, store)
        store.seek(0)
        ret = pd.read_csv(store)
    except Exception as e:
        logger.error(f'copy_expert failed on query: [ {query} ], {e}')
        ret = None
    finally:
        cursor.close()
        connection.close()
        return ret
