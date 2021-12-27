# -*- coding: utf-8 -*-
from findy.interface import Region, Provider, EntityType
from findy.database.schema.fundamental.finance import FinanceBase
from findy.database.plugins.register import register_schema
from findy.database.plugins.eastmoney.finance.china_stock_balance_sheet_recorder import *
from findy.database.plugins.eastmoney.finance.china_stock_cash_flow_recorder import *
from findy.database.plugins.eastmoney.finance.china_stock_finance_factor_recorder import *
from findy.database.plugins.eastmoney.finance.china_stock_income_statement_recorder import *


register_schema(Region.CHN,
                Provider.EastMoney,
                db_name='finance',
                schema_base=FinanceBase,
                entity_type=EntityType.Stock)
