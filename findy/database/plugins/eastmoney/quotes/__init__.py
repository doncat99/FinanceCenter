# -*- coding: utf-8 -*-
from findy.interface import Region, Provider, EntityType
from findy.database.schema.quotes.index.index_1d_kdata import IndexKdataBase
from findy.database.plugins.register import register_schema
from findy.database.plugins.eastmoney.quotes.china_stock_kdata_recorder import *


register_schema(Region.CHN,
                Provider.EastMoney,
                db_name='index_1d_kdata',
                schema_base=IndexKdataBase,
                entity_type=EntityType.Index)
