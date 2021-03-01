# -*- coding: utf-8 -*-
import logging
import cProfile
from io import StringIO
import time
import pstats
import contextlib

import psycopg2
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool

from zvt import zvt_config
from zvt.api.data_type import Region
from zvt.contract import zvt_context


logger = logging.getLogger("zvt.sqltime")


@event.listens_for(Engine, "before_cursor_execute")
def before_cursor_execute(conn, cursor, statement,
                          parameters, context, executemany):
    if zvt_config['debug'] == 2:
        conn.info.setdefault('query_start_time', []).append(time.time())
        logger.debug("Start Query: %s", statement[:50])


@event.listens_for(Engine, "after_cursor_execute")
def after_cursor_execute(conn, cursor, statement,
                         parameters, context, executemany):
    if zvt_config['debug'] == 2:
        total = time.time() - conn.info['query_start_time'].pop(-1)
        logger.debug("Query Complete!")
        logger.debug("Total Time: %f", total)


@contextlib.contextmanager
def profiled():
    pr = cProfile.Profile()
    pr.enable()
    yield
    pr.disable()
    s = StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
    ps.print_stats()
    # uncomment this to see who's calling what
    # ps.print_callers()
    logger.info(s.getvalue())


def build_engine(region: Region):
    database = zvt_context.db_engine_map.get(region)
    if database:
        return database

    # database = await asyncpg.create_pool(
    #     host=zvt_config['db_host'],
    #     port=zvt_config['db_port'],
    #     database="{}_{}".format(zvt_config['db_name'], region.value),
    #     user=zvt_config['db_user'],
    #     password=zvt_config['db_pass'],
    #     min_size=12,
    #     max_size=12)
    db_name = "{}_{}".format(zvt_config['db_name'], region.value)
    link = 'postgresql+psycopg2://{}:{}@{}/{}'.format(
        zvt_config['db_user'], zvt_config['db_pass'], zvt_config['db_host'], db_name)
    database = create_engine(link,
                             encoding='utf-8',
                             echo=False,
                             poolclass=QueuePool,
                             pool_size=25,
                             pool_recycle=7200,
                             pool_pre_ping=True,
                             max_overflow=0,
                            #  server_side_cursors=True,
                             executemany_mode='values',
                             executemany_values_page_size=10000,
                             executemany_batch_page_size=500,
                             )

    with psycopg2.connect(database='postgres', user=zvt_config['db_user'], password=zvt_config['db_pass']) as connection:
        if connection is not None:
            connection.autocommit = True
            cur = connection.cursor()
            cur.execute("SELECT datname FROM pg_database;")
            list_database = cur.fetchall()
            database_name = "{}_{}".format(zvt_config['db_name'], region.value)
            if (database_name,) not in list_database:
                with create_engine('postgresql:///postgres', isolation_level='AUTOCOMMIT').connect() as conn:
                    cmd = "CREATE DATABASE {}".format(database_name)
                    conn.execute(cmd)

    zvt_context.db_engine_map[region] = database
    return database


def to_postgresql(region: Region, df, tablename):
    output = StringIO()
    df.to_csv(output, sep='\t', index=False, header=False, encoding='utf-8')
    output.seek(0)

    db_engine = zvt_context.db_engine_map[region]
    connection = db_engine.raw_connection()
    cursor = connection.cursor()
    try:
        cursor.copy_from(output, tablename, null='', columns=list(df.columns))
        connection.commit()
        cursor.close()
        connection.close()
        return len(df)

    except Exception as e:
        logger.error("copy_from failed on table: [ {} ], {}".format(tablename, e))
    cursor.close()
    connection.close()
    return 0

# async def db_save_table(region: Region, df, tablename):
#     db_engine = zvt_context.db_engine_map[region]

#     async with db_engine.acquire() as conn:
#          async with conn.transaction():
#              tuples = [tuple(x) for x in df.values]
#              await conn.copy_records_to_table(tablename, records=tuples, columns=list(df.columns), timeout=10)


# async def db_delete(region: Region, data_schema: Type[Mixin], filters: List):
#     db_engine = zvt_context.db_engine_map[region]

#     async with db_engine.acquire() as conn:
#         async with conn.transaction():
#             sql = f"delete from {data_schema.__tablename__} where id = '{ids[0]}'"
#             conn.execute(sql)


# the __all__ is generated
__all__ = ['build_engine', 'to_postgresql']
