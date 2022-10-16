import logging
import time
from io import StringIO

import pandas as pd
from sqlalchemy.dialects import postgresql

from findy.interface import Region
from findy.database.context import get_db_engine
from findy.utils.time import PRECISION_STR

logger = logging.getLogger(__name__)


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
    cursor.copy_from(output, tablename, null='', size=1024 * 16, columns=list(df.columns))
    try:        
        connection.commit()
    except Exception as e:
        logger.error(f'copy_from failed on table: [ {tablename} ], {e}')
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
