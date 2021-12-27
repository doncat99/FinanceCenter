# -*- coding: utf-8 -*-
from findy.interface import Region, Provider, EntityType
from findy.database.schema.quotes.index.index_1d_kdata import IndexKdataBase
from findy.database.schema.quotes.stock.stock_1mon_kdata import monKdataBase
from findy.database.schema.quotes.stock.stock_1wk_kdata import wKdataBase
from findy.database.schema.quotes.stock.stock_1d_kdata import dKdataBase
from findy.database.schema.quotes.stock.stock_4h_kdata import fhKdataBase
from findy.database.schema.quotes.stock.stock_1h_kdata import hKdataBase
from findy.database.schema.quotes.stock.stock_30m_kdata import tmKdataBase
from findy.database.schema.quotes.stock.stock_15m_kdata import ofmKdataBase
from findy.database.schema.quotes.stock.stock_5m_kdata import fmKdataBase
from findy.database.schema.quotes.stock.stock_1m_kdata import mKdataBase
from findy.database.plugins.register import register_schema
from findy.database.plugins.yahoo.quotes.yahoo_index_kdata_recorder import *
from findy.database.plugins.yahoo.quotes.yahoo_stock_kdata_recorder import *


register_schema(Region.US,
                Provider.Yahoo,
                db_name='index_1d_kdata',
                schema_base=IndexKdataBase,
                entity_type=EntityType.Index)

register_schema(Region.US,
                Provider.Yahoo,
                db_name='stock_1mon_kdata',
                schema_base=monKdataBase,
                entity_type=EntityType.Stock)

register_schema(Region.US,
                Provider.Yahoo,
                db_name='stock_1wk_kdata',
                schema_base=wKdataBase,
                entity_type=EntityType.Stock)

register_schema(Region.US,
                Provider.Yahoo,
                db_name='stock_1d_kdata',
                schema_base=dKdataBase,
                entity_type=EntityType.Stock)

register_schema(Region.US,
                Provider.Yahoo,
                db_name='stock_4h_kdata',
                schema_base=fhKdataBase,
                entity_type=EntityType.Stock)

register_schema(Region.US,
                Provider.Yahoo,
                db_name='stock_1h_kdata',
                schema_base=hKdataBase,
                entity_type=EntityType.Stock)

register_schema(Region.US,
                Provider.Yahoo,
                db_name='stock_30m_kdata',
                schema_base=tmKdataBase,
                entity_type=EntityType.Stock)

register_schema(Region.US,
                Provider.Yahoo,
                db_name='stock_15m_kdata',
                schema_base=ofmKdataBase,
                entity_type=EntityType.Stock)

register_schema(Region.US,
                Provider.Yahoo,
                db_name='stock_5m_kdata',
                schema_base=fmKdataBase,
                entity_type=EntityType.Stock)

register_schema(Region.US,
                Provider.Yahoo,
                db_name='stock_1m_kdata',
                schema_base=mKdataBase,
                entity_type=EntityType.Stock)
