import logging
from io import StringIO

import pandas as pd

from sqlalchemy.dialects import postgresql

from findy.interface import Region
from findy.database.context import get_db_engine

logger = logging.getLogger(__name__)


def to_postgresql(region: Region, df, tablename):
    saved = 0
    output = StringIO()
    df.to_csv(output, sep='\t', index=False, header=False, encoding='utf-8')
    output.seek(0)

    db_engine = get_db_engine(region)
    connection = db_engine.raw_connection()
    cursor = connection.cursor()
    cursor.copy_from(output, tablename, null='', columns=list(df.columns))

    try:        
        connection.commit()
    except Exception as e:
        logger.error(f'copy_from failed on table: [ {tablename} ], {e}')
        connection.rollback()
    finally:
        saved = len(df)
        cursor.close()
        connection.close()
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
