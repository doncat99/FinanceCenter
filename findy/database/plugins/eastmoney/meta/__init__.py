# -*- coding: utf-8 -*-
from findy.interface import Region, Provider
from findy.database.schema.meta.stock_meta import StockMetaBase
from findy.database.plugins.register import register_schema
from findy.database.plugins.eastmoney.meta.china_stock_category_recorder import *
from findy.database.plugins.eastmoney.meta.china_stock_meta_recorder import *


register_schema(Region.CHN,
                Provider.EastMoney,
                db_name='stock_meta',
                schema_base=StockMetaBase)
