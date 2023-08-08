# -*- coding: utf-8 -*-
from findy.interface import Region, Provider, EntityType
from findy.database.schema.quotes.stock.stock_1mon_kdata import monKdataBase
from findy.database.schema.quotes.stock.stock_1wk_kdata import wKdataBase
from findy.database.schema.quotes.stock.stock_1d_kdata import dKdataBase
from findy.database.schema.register import register_schema
from findy.database.plugins.akshare.quotes.ak_china_stock_kdata_recorder import *


register_schema(Region.CHN,
                Provider.AkShare,
                db_name='stock_1d_kdata',
                schema_base=dKdataBase,
                entity_type=EntityType.Stock)

register_schema(Region.CHN,
                Provider.AkShare,
                db_name='stock_1mon_kdata',
                schema_base=monKdataBase,
                entity_type=EntityType.Stock)

register_schema(Region.CHN,
                Provider.AkShare,
                db_name='stock_1wk_kdata',
                schema_base=wKdataBase,
                entity_type=EntityType.Stock)
