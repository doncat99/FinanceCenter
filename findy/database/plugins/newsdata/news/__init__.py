# -*- coding: utf-8 -*-
from findy.interface import Region, Provider, EntityType
from findy.database.schema.meta.news_meta import NewsMetaBase
from findy.database.schema.register import register_schema
from findy.database.plugins.newsdata.news.news_recorder import *


register_schema(Region.US,
                Provider.NewsData,
                db_name='news',
                schema_base=NewsMetaBase,
                entity_type=EntityType.News)
