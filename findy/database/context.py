# -*- coding: utf-8 -*-
import logging
import os
from io import StringIO
import cProfile
import time
import pstats
import contextlib

from sqlalchemy import create_engine, schema, event, select, exc
from sqlalchemy.engine import Engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.pool import QueuePool
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy_batch_inserts import enable_batch_inserting

from findy import findy_config
from findy.interface import Region, Provider
from findy.database.plugins.register import get_db_name

logger = logging.getLogger(__name__)
logger_time = logging.getLogger("findy.sql.performance")

# provider_dbname -> engine
__db_engine_map = {}

# global sessions
__db_sessions = {}

# db_name -> [declarative_meta1,declarative_meta2...]
__dbname_map_index = {}


@event.listens_for(Engine, "connect")
def connect(dbapi_connection, connection_record):
    connection_record.info['pid'] = os.getpid()


@event.listens_for(Engine, "engine_connect")
def ping_connection(connection, branch):
    if branch:
        # "branch" refers to a sub-connection of a connection,
        # we don't want to bother pinging on these.
        return

    # turn off "close with result".  This flag is only used with
    # "connectionless" execution, otherwise will be False in any case
    save_should_close_with_result = connection.should_close_with_result
    connection.should_close_with_result = False

    try:
        # run a SELECT 1.   use a core select() so that
        # the SELECT of a scalar value without a table is
        # appropriately formatted for the backend
        connection.scalar(select([1]))
    except exc.DBAPIError as err:
        # catch SQLAlchemy's DBAPIError, which is a wrapper
        # for the DBAPI's exception.  It includes a .connection_invalidated
        # attribute which specifies if this connection is a "disconnect"
        # condition, which is based on inspection of the original exception
        # by the dialect in use.
        if err.connection_invalidated:
            # run the same SELECT again - the connection will re-validate
            # itself and establish a new connection.  The disconnect detection
            # here also causes the whole connection pool to be invalidated
            # so that all stale connections are discarded.
            connection.scalar(select([1]))
        else:
            raise
    finally:
        # restore "close with result"
        connection.should_close_with_result = save_should_close_with_result


@event.listens_for(Engine, "checkout")
def checkout(dbapi_connection, connection_record, connection_proxy):
    pid = os.getpid()
    if connection_record.info['pid'] != pid:
        connection_record.connection = connection_proxy.connection = None
        raise exc.DisconnectionError(
                "Connection record belongs to pid %s, "
                "attempting to check out in pid %s" %
                (connection_record.info['pid'], pid))


@event.listens_for(Engine, "before_cursor_execute")
def before_cursor_execute(conn, cursor, statement,
                          parameters, context, executemany):
    if findy_config['debug'] == 2:
        conn.info.setdefault('query_start_time', []).append(time.time())
        logger_time.debug("Start Query: %s", statement[:50])


@event.listens_for(Engine, "after_cursor_execute")
def after_cursor_execute(conn, cursor, statement,
                         parameters, context, executemany):
    if findy_config['debug'] == 2:
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


def create_db(db_name):
    import psycopg2

    # warning: windows platform user need to create postgresql database manually
    connection = psycopg2.connect(database='postgres',
                                  user=findy_config['db_user'],
                                  password=findy_config['db_pass'],
                                  host=findy_config['db_host'],
                                  port=findy_config['db_port'])
    if connection is not None:
        connection.autocommit = True
        try:
            with connection.cursor() as cur:
                cur.execute("SELECT datname FROM pg_database;")
                list_database = cur.fetchall()
                if (db_name,) not in list_database:
                    sql_query = f"CREATE DATABASE {db_name}"
                    cur.execute(sql_query)
                    logger.info(f'async database {db_name} successfully created')
        except Exception as e:
            logger.warning(f'async database {db_name} is not created, with error: {e}')
            raise e
        finally:
            connection.close()
    else:
        logger.error(f'connection not established, async database {db_name} is not created')


def build_engine(region: Region) -> Engine:
    logger.debug(f'start building {region} async database engine...')

    db_name = f"{findy_config['db_name']}_{region.value}"

    create_db(db_name)

    link = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(
        findy_config['db_user'], findy_config['db_pass'],
        findy_config['db_host'], findy_config['db_port'], db_name)
    engine = create_engine(link,
                           encoding='utf-8',
                           echo=False,
                           poolclass=QueuePool,
                           pool_size=10,
                           pool_recycle=300,
                           max_overflow=2,
                           pool_pre_ping=True,
                           pool_use_lifo=True,
                           executemany_mode='values',
                           executemany_values_page_size=10000,
                           executemany_batch_page_size=500)

    logger.debug(f'{region} async engine connect successed')
    return engine


def get_db_engine(region: Region) -> Engine:
    db_engine = __db_engine_map.get(region)
    if not db_engine:
        db_engine = build_engine(region)
        __db_engine_map[region] = db_engine
    return db_engine


def create_index(region: Region, engine, schema_base):
    if not __dbname_map_index.get(region):
        __dbname_map_index[region] = []

    inspector = Inspector.from_engine(engine)

    # create index for 'id', 'timestamp', 'entity_id', 'code', 'report_period', 'updated_timestamp
    for table_name, table in iter(schema_base.metadata.tables.items()):
        if table_name not in __dbname_map_index[region]:
            __dbname_map_index[region].append(table_name)

            # try:
            index_column_names = [index['name'] for index in inspector.get_indexes(table_name)]
            # except Exception as e:
            #     logger.warning(f'create table: {e}')
            #     schema_base.metadata.create_all(engine)
            #     index_column_names = [index['name'] for index in inspector.get_indexes(table_name)]

            logger.debug(f'create async index -> engine: {engine}, table: {table_name}, index: {index_column_names}')

            for col in ['timestamp', 'entity_id', 'code', 'report_period', 'created_timestamp', 'updated_timestamp']:
                if col in table.c:
                    index_name = f'{table_name}_{col}_index'
                    if index_name not in index_column_names:
                        column = eval(f'table.c.{col}')
                        if col == 'timestamp':
                            column = eval(f'table.c.{col}.desc()')
                        else:
                            column = eval(f'table.c.{col}')
                        # index = schema.Index(index_name, column, unique=(col=='id'))
                        index = schema.Index(index_name, column)
                        index.create(engine)

            for cols in [('timestamp', 'entity_id'), ('timestamp', 'code')]:
                if (cols[0] in table.c) and (col[1] in table.c):
                    index_name = f'{table_name}_{col[0]}_{col[1]}_index'
                    if index_name not in index_column_names:
                        column0 = eval(f'table.c.{col[0]}')
                        column1 = eval(f'table.c.{col[1]}')
                        index = schema.Index(index_name, column0, column1)
                        index.create(engine)


def bind_engine(region: Region,
                provider: Provider,
                schema_base: DeclarativeMeta):
    logger.debug(f'bind async engine: {region}, {provider}, {schema_base}')

    # get database engine
    engine = get_db_engine(region)

    # create table
    schema_base.metadata.create_all(engine, checkfirst=True)

    # create index
    create_index(region, engine, schema_base)

    db_session = sessionmaker(bind=engine)()
    enable_batch_inserting(db_session)

    return db_session


def get_db_session(region: Region,
                   provider: Provider,
                   data_schema: object,
                   force_new: bool = False) -> Session:
    db_name = get_db_name(data_schema=data_schema)

    session_key = f'{region.value}_{provider.value}_{db_name}'

    if force_new or not (db_session := __db_sessions.get(session_key)):
        db_session = bind_engine(region, provider, data_schema)
        __db_sessions[session_key] = db_session

    return db_session
