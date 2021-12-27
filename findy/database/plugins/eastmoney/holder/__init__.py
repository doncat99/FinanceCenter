# -*- coding: utf-8 -*-
from findy.interface import Region, Provider, EntityType
from findy.database.schema.misc.holder import HolderBase
from findy.database.plugins.register import register_schema
from findy.database.plugins.eastmoney.holder.top_ten_holder_recorder import *
from findy.database.plugins.eastmoney.holder.top_ten_tradable_holder_recorder import *


register_schema(Region.CHN,
                Provider.EastMoney,
                db_name='holder',
                schema_base=HolderBase,
                entity_type=EntityType.Stock)
