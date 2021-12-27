from findy.interface import Region, Provider
from findy.database.schema.meta.stock_meta import StockMetaBase
from findy.database.schema.misc.overall import OverallBase
from findy.database.plugins.register import register_schema
from findy.database.plugins.exchange.china_etf_list_spider import *
from findy.database.plugins.exchange.china_index_list_spider import *
from findy.database.plugins.exchange.china_stock_list_spider import *
from findy.database.plugins.exchange.china_stock_summary import *
from findy.database.plugins.exchange.us_stock_list_spider import *
from findy.database.plugins.exchange.main_index import *


register_schema(Region.CHN,
                Provider.Exchange,
                db_name='stock_meta',
                schema_base=StockMetaBase)

register_schema(Region.CHN,
                Provider.Exchange,
                db_name='overall',
                schema_base=OverallBase)

register_schema(Region.US,
                Provider.Exchange,
                db_name='stock_meta',
                schema_base=StockMetaBase)
