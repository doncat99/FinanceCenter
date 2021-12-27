# -*- coding: utf-8 -*-
from findy.interface import EntityType
from findy.database.schema import IntervalLevel, AdjustType


def to_bao_trading_level(trading_level: IntervalLevel):

    if trading_level == IntervalLevel.LEVEL_5MIN:
        return '5'
    if trading_level == IntervalLevel.LEVEL_15MIN:
        return '15'
    if trading_level == IntervalLevel.LEVEL_30MIN:
        return '30'
    if trading_level == IntervalLevel.LEVEL_1HOUR:
        return '60'
    if trading_level == IntervalLevel.LEVEL_1DAY:
        return 'd'
    if trading_level == IntervalLevel.LEVEL_1WEEK:
        return 'w'
    if trading_level == IntervalLevel.LEVEL_1MON:
        return 'm'

    raise Exception(f"trading level not support {trading_level}")


def to_bao_trading_field(trading_level):
    if trading_level == 'd':
        return "date, open, high, low, close, preclose, volume, amount, adjustflag, turn, tradestatus, pctChg, peTTM, psTTM, pcfNcfTTM, pbMRQ, isST"
    if trading_level == 'w' or trading_level == 'm':
        return "date, open, high, low, close, volume, amount, adjustflag, turn, pctChg"
    else:
        return "time, open, high, low, close, volume, amount, adjustflag"


def to_bao_entity_id(security_item):
    if security_item.entity_type == EntityType.Stock.value or security_item.entity_type == EntityType.Index.value:
        if security_item.exchange == 'sh':
            return f'sh.{security_item.code}'
        if security_item.exchange == 'sz':
            return f'sz.{security_item.code}'


def to_bao_adjust_flag(adjustflag):
    if adjustflag == AdjustType.bfq:
        return "3"
    if adjustflag == AdjustType.qfq:
        return "2"
    if adjustflag == AdjustType.hfq:
        return "1"


def to_bao_entity_type(entity_type: EntityType):
    if entity_type == EntityType.Stock:
        return "1"
    if entity_type == EntityType.Index:
        return "2"
    if entity_type == EntityType.ETF:
        return "3"


def to_entity_id(bao_code: str, entity_type: EntityType):
    exchange, code = bao_code.split('.')
    return f'{entity_type.value}_{exchange}_{code}'
