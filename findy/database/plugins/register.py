# -*- coding: utf-8 -*-
import logging

from sqlalchemy.ext.declarative import DeclarativeMeta

from findy.interface import Region, Provider, EntityType
from findy.database.schema.datatype import Mixin

logger = logging.getLogger(__name__)

# all registered providers
providers = {}

# db_name -> [declarative_meta1,declarative_meta2...]
__dbname_map_schemas = {}

# db_name -> [declarative_base1,declarative_base2...]
__dbname_map_base = {}

# all registered schemas
__schemas = []


def get_schema_by_name(name: str) -> DeclarativeMeta:
    for schema in __schemas:
        if schema.__name__ == name:
            return schema


def get_schema_columns(schema: DeclarativeMeta) -> object:
    return schema.__table__.columns.keys()


def get_db_name(data_schema: DeclarativeMeta) -> str:
    for db_name, base in __dbname_map_base.items():
        if issubclass(data_schema, base):
            return db_name


def register_schema(region: Region,
                    provider: Provider,
                    db_name: str,
                    schema_base: DeclarativeMeta,
                    entity_type: EntityType = None):
    local_schemas = []

    # for item in schema_base._decl_class_registry.items():
    for item in schema_base.registry.mappers:
        cls = item.class_
        if type(cls) == DeclarativeMeta:
            # register provider to the schema
            if issubclass(cls, Mixin):
                cls.register_provider(region, provider)

            if __dbname_map_schemas.get(db_name):
                local_schemas = __dbname_map_schemas[db_name]
            __schemas.append(cls)
            local_schemas.append(cls)

    __dbname_map_schemas[db_name] = local_schemas

    if region in providers.keys():
        if provider not in providers[region]:
            providers[region].append(provider)
    else:
        providers.update({region: [provider]})

    __dbname_map_base[db_name] = schema_base
