# -*- coding: utf-8 -*-
from findy.interface import Region, Provider
from findy.database.schema.meta.stock_meta import StockMetaBase
from findy.database.schema.quotes.trade_day import TradeDayBase
from findy.database.plugins.register import register_schema
from findy.database.plugins.baostock.meta.bao_china_stock_meta_recorder import *
from findy.database.plugins.baostock.meta.bao_china_stock_trade_day_recorder import *


register_schema(Region.CHN,
                Provider.BaoStock,
                db_name='stock_meta',
                schema_base=StockMetaBase)

register_schema(Region.CHN,
                Provider.BaoStock,
                db_name='trade_day',
                schema_base=TradeDayBase)
