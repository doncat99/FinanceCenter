import logging
from typing import List, Type

import pandas as pd

from findy.interface import Region, Provider
from findy.database.persist import df_to_db
from findy.database.schema.datatype import Mixin


logger = logging.getLogger(__name__)


async def write_data(region: Region,
                     provider: Provider,
                     data_schema,
                     db_session,
                     df: pd.DataFrame,
                     ref_entity = None,
                     drop_duplicates: bool = True,
                     fix_duplicate_way: str = 'ignore',
                     force_update=False) -> object:
    await df_to_db(region, provider, data_schema, db_session, df, ref_entity, drop_duplicates, fix_duplicate_way, force_update)


def del_data(db_session, data_schema: Type[Mixin], filters: List = None):
    query = db_session.query(data_schema)
    if filters:
        for f in filters:
            query = query.filter(f)
    query.delete()

    try:
        db_session.execute(query)
    except Exception as e:
        logger.error(f"query {data_schema.__tablename__} failed with error: {e}")

    try:
        db_session.commit()
    except Exception as e:
        logger.error(f'df_to_db {data_schema.__tablename__}, error: {e}')
        db_session.rollback()
    finally:
        db_session.close()
