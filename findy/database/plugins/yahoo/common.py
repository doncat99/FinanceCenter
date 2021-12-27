# -*- coding: utf-8 -*-
from findy.database.schema import IntervalLevel, ReportPeriod


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


def to_report_period_type(report_date):
    if report_date == 0:
        return ReportPeriod.season1.value
    if report_date == 1:
        return ReportPeriod.half_year.value
    if report_date == 2:
        return ReportPeriod.season3.value
    if report_date == 3:
        return ReportPeriod.year.value
    return None
