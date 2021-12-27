# -*- coding: utf-8 -*-
from findy.interface import Region, Provider, EntityType
from findy.database.schema.fundamental.finance import FinanceBase
from findy.database.plugins.register import register_schema
from findy.database.plugins.yahoo.finance.us_stock_balance_sheet_recorder import *


register_schema(Region.US,
                Provider.Yahoo,
                db_name='finance',
                schema_base=FinanceBase,
                entity_type=EntityType.Stock)
