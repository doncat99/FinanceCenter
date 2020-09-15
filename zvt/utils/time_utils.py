# -*- coding: utf-8 -*-
import datetime
import math

import arrow
import pandas as pd
import tzlocal
import pytz

from zvt.contract import IntervalLevel
from zvt.contract.common import Region


CHINA_TZ = 'Asia/Shanghai'
US_TZ = 'America/New_York'

TIME_FORMAT_ISO8601 = "YYYY-MM-DDTHH:mm:ss.SSS"

TIME_FORMAT_DAY = 'YYYY-MM-DD'

TIME_FORMAT_DAY1 = 'YYYYMMDD'

TIME_FORMAT_MINUTE = 'YYYYMMDDHHmm'

TIME_FORMAT_MINUTE1 = 'HH:mm'

TIME_FORMAT_MINUTE2 = "YYYY-MM-DD HH:mm:ss"


def is_datetime(the_time):
    return isinstance(the_time, datetime.datetime)

# ms(int) or second(float) or str
def to_pd_timestamp(the_time):
    if the_time is None:
        return None
    if type(the_time) == int:
        return pd.Timestamp.fromtimestamp(the_time / 1000)

    if type(the_time) == float:
        return pd.Timestamp.fromtimestamp(the_time)

    return pd.Timestamp(the_time)

def to_timestamp(the_time):
    return int(to_pd_timestamp(the_time).tz_localize(tzlocal.get_localzone()).timestamp() * 1000)


def now_timestamp():
    return int(pd.Timestamp.utcnow().timestamp() * 1000)


def now_pd_timestamp(region: Region) -> pd.Timestamp:
    if region == Region.US:
        tz = pytz.timezone('America/New_York')
        return pd.Timestamp.now(tz).replace(tzinfo=None)
    return pd.Timestamp.now()


def to_time_str(the_time, fmt=TIME_FORMAT_DAY):
    try:
        return arrow.get(to_pd_timestamp(the_time)).format(fmt)
    except Exception as _:
        return the_time


def now_time_str(region: Region, fmt=TIME_FORMAT_DAY):
    return to_time_str(the_time=now_pd_timestamp(region), fmt=fmt)


def next_date(the_time, days=1):
    return to_pd_timestamp(the_time) + datetime.timedelta(days=days)


def is_same_date(one, two):
    return to_pd_timestamp(one).date() == to_pd_timestamp(two).date()

def date_delta(one, two):
    return (to_pd_timestamp(one).date() - to_pd_timestamp(two).date()).days

def is_same_time(one, two):
    return to_timestamp(one) == to_timestamp(two)


def get_year_quarter(time):
    time = to_pd_timestamp(time)
    return time.year, ((time.month - 1) // 3) + 1


def day_offset_today(region: Region, offset=0):
    return now_pd_timestamp(region) + datetime.timedelta(days=offset)


def get_year_quarters(start, end):
    start_year_quarter = get_year_quarter(start)
    current_year_quarter = get_year_quarter(end)
    if current_year_quarter[0] == start_year_quarter[0]:
        return [(current_year_quarter[0], x) for x in range(start_year_quarter[1], current_year_quarter[1] + 1)]
    elif current_year_quarter[0] - start_year_quarter[0] == 1:
        return [(start_year_quarter[0], x) for x in range(start_year_quarter[1], 5)] + \
               [(current_year_quarter[0], x) for x in range(1, current_year_quarter[1] + 1)]
    elif current_year_quarter[0] - start_year_quarter[0] > 1:
        return [(start_year_quarter[0], x) for x in range(start_year_quarter[1], 5)] + \
               [(x, y) for x in range(start_year_quarter[0] + 1, current_year_quarter[0]) for y in range(1, 5)] + \
               [(current_year_quarter[0], x) for x in range(1, current_year_quarter[1] + 1)]
    else:
        raise Exception("wrong start time:{}".format(start))


def date_and_time(the_date, the_time):
    time_str = '{}T{}:00.000'.format(to_time_str(the_date), the_time)
    return to_pd_timestamp(time_str)


def convert_timedelta(duration):
    days, seconds = duration.days, duration.seconds
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = (seconds % 60)
    return days, hours, minutes, seconds


def time_delta(enter, exit):
    enter_delta = datetime.timedelta(hours=enter.hour, minutes=enter.minute, seconds=enter.second)
    exit_delta = datetime.timedelta(hours=exit.hour, minutes=exit.minute, seconds=exit.second)
    difference_delta = exit_delta - enter_delta
    return convert_timedelta(difference_delta)


def count_mins_from_day(the_time):
    rest = datetime.time(hour=11, minute=30)
    end = datetime.time(hour=15)

    if the_time < rest:
        _, _, minutes, _ = time_delta(the_time, rest)
        return minutes + 120
    else:
        _, _, minutes, _ = time_delta(the_time, end)
        return minutes


def count_hours_from_day(the_time):
    rest = datetime.time(hour=11, minute=30)
    end = datetime.time(hour=15)

    if the_time < rest:
        _, hours, _, _ = time_delta(the_time, rest)
        return hours + 2
    else:
        _, hours, _, _ = time_delta(the_time, end)
        return hours

def count_mins_before_close_time(now, close_hour, close_minute):
    close_time = datetime.time(hour=close_hour, minute=close_minute)
    now_time = datetime.time(hour=now.hour, minute=now.minute, second=now.second)
    _, hours, minutes, _ = time_delta(now_time, close_time)
    return minutes + hours*60
        
def next_timestamp(current_timestamp: pd.Timestamp, level: IntervalLevel) -> pd.Timestamp:
    current_timestamp = to_pd_timestamp(current_timestamp)
    return current_timestamp + pd.Timedelta(seconds=level.to_second())


def evaluate_size_from_timestamp(start_timestamp: pd.Timestamp,
                                 end_timestamp: pd.Timestamp,
                                 level: IntervalLevel,
                                 one_day_trading_minutes,
                                 trade_day = None):
    """
    given from timestamp,level,one_day_trading_minutes,this func evaluate size of kdata to current.
    it maybe a little bigger than the real size for fetching all the kdata.

    :param start_timestamp:
    :type start_timestamp: pd.Timestamp
    :param level:
    :type level: IntervalLevel
    :param one_day_trading_minutes:
    :type one_day_trading_minutes: int
    """
    # if not end_timestamp:
    #     end_timestamp = now_pd_timestamp()
    # else:
    #     end_timestamp = to_pd_timestamp(end_timestamp)

    time_delta = end_timestamp - to_pd_timestamp(start_timestamp)

    one_day_trading_seconds = one_day_trading_minutes * 60

    if level == IntervalLevel.LEVEL_1MON:
        if trade_day is not None:
            try:
                size = int(math.ceil(trade_day.index(start_timestamp) / 22))
                size = 0 if size == 0 else size + 1
                return size
            except ValueError as _:
                if start_timestamp < trade_day[-1]:
                    return int(math.ceil(len(trade_day) / 22))
                # raise Exception("wrong start time:{}, error:{}".format(start_timestamp, e))
        return int(math.ceil(time_delta.days / 30))

    if level == IntervalLevel.LEVEL_1WEEK:
        if trade_day is not None:
            try:
                size = int(math.ceil(trade_day.index(start_timestamp) / 5))
                size = 0 if size == 0 else size + 1
                return size
            except ValueError as _:
                if start_timestamp < trade_day[-1]:
                    return int(math.ceil(len(trade_day) / 5))
                # raise Exception("wrong start time:{}, error:{}".format(start_timestamp, e))
        return int(math.ceil(time_delta.days / 7))

    if level == IntervalLevel.LEVEL_1DAY:
        if trade_day is not None and len(trade_day) > 0:
            try:
                return trade_day.index(start_timestamp)
            except ValueError as _:
                if start_timestamp < trade_day[-1]:
                    return len(trade_day)
                # raise Exception("wrong start time:{}, error:{}".format(start_timestamp, e))
        return time_delta.days

    if level == IntervalLevel.LEVEL_1HOUR:
        if trade_day is not None:
            start_date = start_timestamp.replace(hour=0, minute=0, second=0)
            try:
                days = trade_day.index(start_date)
                time = datetime.datetime.time(start_timestamp)
                size = (days)*4 + int(math.ceil(count_hours_from_day(time)))
                return size
            except ValueError as _:
                if start_date < trade_day[-1]:
                    return len(trade_day)*4
                # raise Exception("wrong start time:{}, error:{}".format(start_timestamp, e))
        return int(math.ceil(time_delta.days * 4 * 2))

    if level == IntervalLevel.LEVEL_30MIN:
        if trade_day is not None:
            start_date = start_timestamp.replace(hour=0, minute=0, second=0)
            try:
                days = trade_day.index(start_date)
                time = datetime.datetime.time(start_timestamp)
                size = (days)*4*2 + int(math.ceil(count_mins_from_day(time) / 5))
                return size
            except ValueError as _:
                if start_date < trade_day[-1]:
                    return len(trade_day)*4*2
                # raise Exception("wrong start time:{}, error:{}".format(start_timestamp, e))
        return int(math.ceil(time_delta.days * 4 * 2))

    if level == IntervalLevel.LEVEL_15MIN:
        if trade_day is not None:
            start_date = start_timestamp.replace(hour=0, minute=0, second=0)
            try:
                days = trade_day.index(start_date)
                time = datetime.datetime.time(start_timestamp)
                size = (days)*4*4 + int(math.ceil(count_mins_from_day(time) / 5))
                return size
            except ValueError as _:
                if start_date < trade_day[-1]:
                    return len(trade_day)*4*4
                # raise Exception("wrong start time:{}, error:{}".format(start_timestamp, e))
        return int(math.ceil(time_delta.days * 4 * 4))

    if level == IntervalLevel.LEVEL_5MIN:
        if trade_day is not None:
            start_date = start_timestamp.replace(hour=0, minute=0, second=0)
            try:
                days = trade_day.index(start_date)
                time = datetime.datetime.time(start_timestamp)
                size = (days)*4*12 + int(math.ceil(count_mins_from_day(time) / 5))
                return size
            except ValueError as _:
                if start_date < trade_day[-1]:
                    return len(trade_day)*4*12
                # raise Exception("wrong start time:{}, error:{}".format(start_timestamp, e))
        return int(math.ceil(time_delta.days * 4 * 12))

    if level == IntervalLevel.LEVEL_1MIN:
        if trade_day is not None:
            start_date = start_timestamp.replace(hour=0, minute=0, second=0)
            try:
                days = trade_day.index(start_date)
                time = datetime.datetime.time(start_timestamp)
                size = (days)*4*60 + count_mins_from_day(time)
                return size
            except ValueError as _:
                if start_date < trade_day[-1]:
                    return len(trade_day)*4*60
                # raise Exception("wrong start time:{}, error:{}".format(start_timestamp, e))
        return int(math.ceil(time_delta.days * 4 * 60))

    if time_delta.days > 0:
        seconds = (time_delta.days + 1) * one_day_trading_seconds
        return int(math.ceil(seconds / level.to_second()))
    else:
        seconds = time_delta.total_seconds()
        return min(int(math.ceil(seconds / level.to_second())),
                   one_day_trading_seconds / level.to_second())


def is_finished_kdata_timestamp(timestamp, level: IntervalLevel):
    timestamp = to_pd_timestamp(timestamp)
    if level.floor_timestamp(timestamp) == timestamp:
        return True
    return False


def is_in_same_interval(t1: pd.Timestamp, t2: pd.Timestamp, level: IntervalLevel):
    t1 = to_pd_timestamp(t1)
    t2 = to_pd_timestamp(t2)
    if level == IntervalLevel.LEVEL_1WEEK:
        return t1.week == t2.week
    if level == IntervalLevel.LEVEL_1MON:
        return t1.month == t2.month

    return level.floor_timestamp(t1) == level.floor_timestamp(t2)


if __name__ == '__main__':
    print(date_and_time('2019-10-01', '10:00'))
