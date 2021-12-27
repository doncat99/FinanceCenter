# -*- coding: utf-8 -*-
from findy.interface import Region, Provider, EntityType
from findy.database.schema.fundamental.dividend_financing import DividendFinancingBase
from findy.database.plugins.register import register_schema
from findy.database.plugins.eastmoney.dividend_financing.dividend_detail_recorder import *
from findy.database.plugins.eastmoney.dividend_financing.dividend_financing_recorder import *
from findy.database.plugins.eastmoney.dividend_financing.rights_issue_detail_recorder import *
from findy.database.plugins.eastmoney.dividend_financing.spo_detail_recorder import *


register_schema(Region.CHN,
                Provider.EastMoney,
                db_name='dividend_financing',
                schema_base=DividendFinancingBase,
                entity_type=EntityType.Stock)
