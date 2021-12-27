import logging
from io import StringIO

import pandas as pd

from sqlalchemy.dialects import postgresql

from findy.interface import Region

logger = logging.getLogger(__name__)

# provider_dbname -> engine
__db_sync_engine_map = {}


def build_engine(region: Region):
    from sqlalchemy import create_engine
    # from sqlalchemy.pool import QueuePool

    from findy import findy_config

    logger.debug(f'start building {region} sync database engine...')

    db_name = f"{findy_config['db_name']}_{region.value}"

    link = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(
        findy_config['db_user'], findy_config['db_pass'],
        findy_config['db_host'], findy_config['db_port'], db_name)
    engine = create_engine(link,
                           encoding='utf-8',
                           echo=False)
                        #    poolclass=QueuePool,
                        #    pool_size=10,
                        #    pool_recycle=300,
                        #    max_overflow=2,
                        #    pool_pre_ping=True,
                        #    pool_use_lifo=True,
                        #    executemany_mode='values',
                        #    executemany_values_page_size=10000,
                        #    executemany_batch_page_size=500)

    logger.debug(f'{region} sync engine connect successed')
    return engine


def get_db_engine(region: Region,
                  db_name: str = None):
    db_engine = __db_sync_engine_map.get(region)
    if db_engine:
        logger.debug(f'sync engine cache hit: engine key: {region.value}_{db_name}')
        return db_engine

    logger.debug(f'create sync engine key: {region.value}_{db_name}')
    __db_sync_engine_map[region] = build_engine(region)
    return __db_sync_engine_map[region]


def to_postgresql(region: Region, df, tablename):
    output = StringIO()
    df.to_csv(output, sep='\t', index=False, header=False, encoding='utf-8')
    output.seek(0)

    db_engine = get_db_engine(region)
    connection = db_engine.raw_connection()
    cursor = connection.cursor()
    try:
        cursor.copy_from(output, tablename, null='', columns=list(df.columns))
        connection.commit()
        cursor.close()
        connection.close()
        return len(df)
    except Exception as e:
        logger.error(f'copy_from failed on table: [ {tablename} ], {e}')
    cursor.close()
    connection.close()
    return 0


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
        return pd.read_csv(store)
    except Exception as e:
        logger.error(f'copy_expert failed on query: [ {query} ], {e}')
    cursor.close()
    connection.close()
    return None
