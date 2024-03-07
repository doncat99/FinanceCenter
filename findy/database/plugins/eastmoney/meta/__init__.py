# -*- coding: utf-8 -*-
from findy.interface import Region, Provider, EntityType
from findy.database.schema.meta.news_meta import NewsMetaBase
from findy.database.schema.register import register_schema
from findy.database.plugins.eastmoney.meta.chn_news_meta_recorder import *


register_schema(Region.CHN,
                Provider.EastMoney,
                db_name='news_title',
                schema_base=NewsMetaBase,
                entity_type=EntityType.News)


register_schema(Region.CHN,
                Provider.EastMoney,
                db_name='news_content',
                schema_base=NewsMetaBase,
                entity_type=EntityType.News)