# -*- coding: utf-8 -*-
import logging

from sqlalchemy.ext.declarative import DeclarativeMeta

from findy.interface import Region, Provider, EntityType
from findy.database.schema.datatype import Mixin, EntityMixin

logger = logging.getLogger(__name__)

# all registered providers
providers = {}

# db_name -> [declarative_meta1,declarative_meta2...]
__dbname_map_schemas = {}

# db_name -> [declarative_base1,declarative_base2...]
__dbname_map_base = {}

# all registered schemas
__schemas = []

# all registered entity types
__entity_types = []

# entity_type -> entity schema
__entity_schema_map = {}


def register_entity(entity_type: EntityType = None):
    def register(cls):
        # register the entity
        if issubclass(cls, EntityMixin):
            entity_type_ = entity_type
            if not entity_type:
                entity_type_ = EntityType(cls.__name__.lower())

            if entity_type_ not in __entity_types:
                __entity_types.append(entity_type_)
            __entity_schema_map[entity_type_] = cls
        return cls
    return register


def get_entity_schema_by_type(entity_type: EntityType):
    return __entity_schema_map[entity_type]


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