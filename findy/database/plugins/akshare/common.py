# -*- coding: utf-8 -*-
from findy.interface import EntityType
from findy.database.schema import IntervalLevel, AdjustType


def to_ak_trading_level(trading_level: IntervalLevel):
    if trading_level == IntervalLevel.LEVEL_1DAY:
        return 'daily'
    if trading_level == IntervalLevel.LEVEL_1WEEK:
        return 'weekly'
    if trading_level == IntervalLevel.LEVEL_1MON:
        return 'monthly'

    raise Exception(f"trading level not support {trading_level}")


def to_ak_trading_field(trading_level):
    if trading_level == 'd':
        return "time, open, close, high, low, volume, amount, amplitude, pct_chg, change, turnover, tic"
    if trading_level == 'w' or trading_level == 'm':
        return "time, open, close, high, low, volume, amount, amplitude, pct_chg, change, turnover, tic"
    else:
        return "time, open, close, high, low, volume, amount, amplitude, pct_chg, change, turnover, tic"


def to_ak_entity_id(security_item):
    if security_item.entity_type == EntityType.Stock.value or security_item.entity_type == EntityType.Index.value:
        return f'{security_item.code}'


def to_ak_adjust_flag(adjustflag):
    if adjustflag == AdjustType.bfq:
        return ""
    if adjustflag == AdjustType.qfq:
        return "qfq"
    if adjustflag == AdjustType.hfq:
        return "hfq"


def to_bao_entity_type(entity_type: EntityType):
    if entity_type == EntityType.Stock:
        return "1"
    if entity_type == EntityType.Index:
        return "2"
    if entity_type == EntityType.Fund:
        return "3"


def to_entity_id(bao_code: str, entity_type: EntityType):
    exchange, code = bao_code.split('.')
    return f'{entity_type.value}_{exchange}_{code}'
