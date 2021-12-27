# -*- coding: utf-8 -*-
import datetime

import pandas as pd
import tzlocal
import pytz

from findy.interface import Region

tz_list = {Region.CHN: 'Asia/Shanghai',
           Region.US: 'America/New_York'}

TIME_FORMAT_ISO8601 = "YYYY-MM-DDTHH:mm:ss.SSS"
TIME_FORMAT_DAY = 'YYYY-MM-DD'
TIME_FORMAT_DAY1 = 'YYYYMMDD'
TIME_FORMAT_MINUTE = 'YYYYMMDDHHmm'
TIME_FORMAT_MINUTE1 = 'HH:mm'
TIME_FORMAT_MINUTE2 = "YYYY-MM-DD HH:mm:ss"
PD_TIME_FORMAT_ISO8601 = '%Y-%m-%dT%H:%M%:%SZ'
PD_TIME_FORMAT_DAY = '%Y-%m-%d'
PRECISION_STR = '{' + ':>{},.{}f'.format(6, 2) + '}'


def to_pd_timestamp(the_time) -> pd.Timestamp:
    if the_time is None:
        return None

    if type(the_time) == int:
        return pd.Timestamp.fromtimestamp(the_time / 1000)

    if type(the_time) == float:
        return pd.Timestamp.fromtimestamp(the_time)

    return pd.Timestamp(the_time)


def to_time_str(the_time, fmt=PD_TIME_FORMAT_DAY) -> str:
    if the_time is None:
        return the_time

    if type(the_time) == str:
        return the_time

    assert isinstance(the_time, pd.Timestamp) or isinstance(the_time, datetime.datetime)
    return the_time.strftime(fmt)


def to_timestamp(the_time):
    return int(to_pd_timestamp(the_time).tz_localize(tzlocal.get_localzone()).timestamp() * 1000)


def to_pd_datetime(the_date, the_time):
    time_str = f'{to_time_str(the_date)}T{the_time}:00.000'
    return to_pd_timestamp(time_str)


def now_pd_timestamp(region: Region) -> pd.Timestamp:
    central = pytz.timezone(tz_list[region])
    # return central.localize(d)
    return pd.Timestamp.now(central).replace(tzinfo=None)


def now_time_str(region: Region, fmt=TIME_FORMAT_DAY):
    return to_time_str(the_time=now_pd_timestamp(region), fmt=fmt)


def next_dates(the_time, days=1) -> pd.Timestamp:
    return to_pd_timestamp(the_time) + datetime.timedelta(days=days)


def is_same_date(one, two) -> bool:
    return to_pd_timestamp(one).date() == to_pd_timestamp(two).date()


def is_same_time(one, two):
    return to_timestamp(one) == to_timestamp(two)
