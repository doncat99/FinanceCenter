# -*- coding: utf-8 -*-
from zvt.contract import IntervalLevel


def to_yahoo_trading_level(trading_level: IntervalLevel):
    if trading_level < IntervalLevel.LEVEL_1HOUR:
        return trading_level.value

    if trading_level == IntervalLevel.LEVEL_1HOUR:
        return '60m'
    if trading_level == IntervalLevel.LEVEL_1DAY:
        return '1d'
    if trading_level == IntervalLevel.LEVEL_1WEEK:
        return '1wk'
    if trading_level == IntervalLevel.LEVEL_1MON:
        return '1mo'
