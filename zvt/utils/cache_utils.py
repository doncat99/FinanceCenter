# -*- coding: utf-8 -*-
import os
import pickle
from datetime import datetime, timedelta

from zvt import zvt_env
from zvt.api.data_type import Region


def valid(region: Region, func_name, valid_time, data):
    key = "{}_{}".format(region.value, func_name)
    lasttime = data.get(key, None)
    if lasttime is not None:
        if lasttime > (datetime.now() - timedelta(hours=valid_time)):
            return True
    return False


def get_cache():
    file = zvt_env['cache_path'] + '/' + 'cache.pkl'
    if os.path.exists(file) and os.path.getsize(file) > 0:
        with open(file, 'rb') as handle:
            return pickle.load(handle)
    return {}


def dump_cache(region: Region, func_name, data):
    key = "{}_{}".format(region.value, func_name)
    file = zvt_env['cache_path'] + '/' + 'cache.pkl'
    with open(file, 'wb+') as handle:
        data.update({key: datetime.now()})
        pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)
