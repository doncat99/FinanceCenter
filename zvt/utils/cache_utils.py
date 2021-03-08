# -*- coding: utf-8 -*-
import os
import pickle
from datetime import datetime, timedelta
from functools import wraps, lru_cache
import json

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


def hashable_lru(func):
    cache = lru_cache(maxsize=None)

    def deserialise(value):
        try:
            return json.loads(value)
        except Exception:
            return value

    def func_with_serialized_params(*args, **kwargs):
        _args = tuple([deserialise(arg) for arg in args])
        _kwargs = {k: deserialise(v) for k, v in kwargs.items()}
        return func(*_args, **_kwargs)

    cached_function = cache(func_with_serialized_params)

    @wraps(func)
    def lru_decorator(*args, **kwargs):
        _args = tuple([json.dumps(arg, sort_keys=True) if type(arg) in (list, dict) else arg for arg in args])
        _kwargs = {k: json.dumps(v, sort_keys=True) if type(v) in (list, dict) else v for k, v in kwargs.items()}
        return cached_function(*_args, **_kwargs)
    lru_decorator.cache_info = cached_function.cache_info
    lru_decorator.cache_clear = cached_function.cache_clear
    return lru_decorator
