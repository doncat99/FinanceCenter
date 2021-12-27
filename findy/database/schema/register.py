# -*- coding: utf-8 -*-
import logging

from findy.interface import EntityType
from findy.database.schema.datatype import EntityMixin

logger = logging.getLogger(__name__)

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
