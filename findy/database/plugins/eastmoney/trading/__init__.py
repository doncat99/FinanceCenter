# -*- coding: utf-8 -*-
from findy.interface import Region, Provider, EntityType
from findy.database.schema.fundamental.trading import TradingBase
from findy.database.plugins.register import register_schema
from findy.database.plugins.eastmoney.trading.holder_trading_recorder import *
from findy.database.plugins.eastmoney.trading.manager_trading_recorder import *


register_schema(Region.CHN,
                Provider.EastMoney,
                db_name='trading',
                schema_base=TradingBase,
                entity_type=EntityType.Stock)
