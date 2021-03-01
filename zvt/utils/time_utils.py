# -*- coding: utf-8 -*-
import datetime
import math

import arrow
import pandas as pd
import tzlocal
import pytz

from zvt.api.data_type import Region
from zvt.contract import IntervalLevel

CHINA_TZ = 'Asia/Shanghai'
US_TZ = 'America/New_York'

TIME_FORMAT_ISO8601 = "YYYY-MM-DDTHH:mm:ss.SSS"

TIME_FORMAT_DAY = 'YYYY-MM-DD'

TIME_FORMAT_DAY1 = 'YYYYMMDD'

TIME_FORMAT_MINUTE = 'YYYYMMDDHHmm'

TIME_FORMAT_MINUTE1 = 'HH:mm'

TIME_FORMAT_MINUTE2 = "YYYY-MM-DD HH:mm:ss"

PD_TIME_FORMAT_ISO8601 = '%Y-%m-%dT%H:%M%:%SZ'

PD_TIME_FORMAT_DAY = '%Y-%m-%d'

none_values = ['--']


def is_datetime(the_time):
    return isinstance(the_time, datetime.datetime)


# ms(int) or second(float) or str
def to_pd_timestamp(the_time):
    if the_time is None or the_time in none_values:
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


def to_time_int(the_time):
    return int(datetime.datetime.strptime(the_time, PD_TIME_FORMAT_DAY).timestamp())


def to_time_str(the_time, fmt=TIME_FORMAT_DAY):
    try:
        return arrow.get(to_pd_timestamp(the_time)).format(fmt)
    except:
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


def next_timestamp(current_timestamp: pd.Timestamp, level: IntervalLevel) -> pd.Timestamp:
    current_timestamp = to_pd_timestamp(current_timestamp)
    return current_timestamp + pd.Timedelta(seconds=level.to_second())


def eval_size_of_timestamp(start_timestamp: pd.Timestamp,
                           end_timestamp: pd.Timestamp,
                           level: IntervalLevel,
                           one_day_trading_minutes):
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
    assert end_timestamp is not None

    time_delta = end_timestamp - to_pd_timestamp(start_timestamp)

    one_day_trading_seconds = one_day_trading_minutes * 60

    if level == IntervalLevel.LEVEL_1DAY:
        return time_delta.days

    if level == IntervalLevel.LEVEL_1WEEK:
        return int(math.ceil(time_delta.days / 7))

    if level == IntervalLevel.LEVEL_1MON:
        return int(math.ceil(time_delta.days / 30))

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


# the __all__ is generated
__all__ = ['to_pd_timestamp', 'to_timestamp', 'now_timestamp', 'now_pd_timestamp',
           'to_time_str', 'now_time_str', 'next_date', 'is_same_date', 'is_same_time',
           'get_year_quarter', 'day_offset_today', 'get_year_quarters', 'date_and_time',
           'next_timestamp', 'eval_size_of_timestamp', 'is_finished_kdata_timestamp',
           'is_in_same_interval']
