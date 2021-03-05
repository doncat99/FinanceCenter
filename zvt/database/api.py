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
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy_batch_inserts import enable_batch_inserting

from zvt import zvt_config
from zvt.api.data_type import Region, Provider

logger = logging.getLogger(__name__)
logger_time = logging.getLogger("zvt.sqltime")

# provider_dbname -> engine
db_engine_map = {}

# provider_dbname -> session
db_session_map = {}

# global sessions
sessions = {}

# db_name -> [declarative_base1,declarative_base2...]
dbname_map_base = {}


@event.listens_for(Engine, "before_cursor_execute")
def before_cursor_execute(conn, cursor, statement,
                          parameters, context, executemany):
    if zvt_config['debug'] == 2:
        conn.info.setdefault('query_start_time', []).append(time.time())
        logger_time.debug("Start Query: %s", statement[:50])


@event.listens_for(Engine, "after_cursor_execute")
def after_cursor_execute(conn, cursor, statement,
                         parameters, context, executemany):
    if zvt_config['debug'] == 2:
        total = time.time() - conn.info['query_start_time'].pop(-1)
        logger_time.debug("Query Complete!")
        logger_time.debug("Total Time: %f", total)


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
    logger_time.info(s.getvalue())


def build_engine(region: Region) -> Engine:
    logger.info(f"start building {region} database engine...")

    # engine = await asyncpg.create_pool(
    #     host=zvt_config['db_host'],
    #     port=zvt_config['db_port'],
    #     database="{}_{}".format(zvt_config['db_name'], region.value),
    #     user=zvt_config['db_user'],
    #     password=zvt_config['db_pass'],
    #     min_size=12,
    #     max_size=12)
    db_name = "{}_{}".format(zvt_config['db_name'], region.value)
    link = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(
        zvt_config['db_user'], zvt_config['db_pass'], zvt_config['db_host'], zvt_config['db_port'], db_name)
    engine = create_engine(link,
                           encoding='utf-8',
                           echo=False,
                           poolclass=QueuePool,
                           pool_size=0,
                           pool_recycle=-1,
                           max_overflow=-1,
                           executemany_mode='values',
                           executemany_values_page_size=10000,
                           executemany_batch_page_size=500)

    try:
        with psycopg2.connect(database='postgres', user=zvt_config['db_user'], password=zvt_config['db_pass'],
                              host=zvt_config['db_host'], port=zvt_config['db_port']) as connection:
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
    except:
        pass

    logger.info(f"{region} engine connect successed")

    return engine


def to_postgresql(region: Region, df, tablename):
    output = StringIO()
    df.to_csv(output, sep='\t', index=False, header=False, encoding='utf-8')
    output.seek(0)

    db_engine = db_engine_map.get(region)
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


def get_db_engine(region: Region,
                  db_name: str = None) -> Engine:
    db_engine = db_engine_map.get(region)
    if db_engine:
        logger.debug("engine cache hit: engine key: {}_{}".format(region.value, db_name))
        return db_engine

    logger.debug("create engine key: {}_{}".format(region.value, db_name))
    db_engine_map[region] = build_engine(region)
    return db_engine_map[region]


def get_db_session_factory(region: Region,
                           provider: Provider,
                           db_name: str = None):
    session_key = '{}_{}_{}'.format(region.value, provider.value, db_name)
    session = db_session_map.get(session_key)
    if not session:
        session = sessionmaker()
        db_session_map[session_key] = session
    return session


def get_db_session(region: Region,
                   provider: Provider,
                   db_name: str = None,
                   data_schema: object = None,
                   force_new: bool = False) -> Session:
    if data_schema:
        db_name = get_db_name(data_schema=data_schema)

    session_key = '{}_{}_{}'.format(region.value, provider.value, db_name)

    if force_new:
        return get_db_session_factory(region, provider, db_name)()

    session = sessions.get(session_key)
    if not session:
        session = get_db_session_factory(region, provider, db_name)()
        enable_batch_inserting(session)
        sessions[session_key] = session
    return session


def get_db_name(data_schema: DeclarativeMeta) -> str:
    for db_name, base in dbname_map_base.items():
        if issubclass(data_schema, base):
            return db_name


def set_db_name(db_name, schema_base) -> str:
    dbname_map_base[db_name] = schema_base


# the __all__ is generated
__all__ = ['build_engine', 'to_postgresql']
