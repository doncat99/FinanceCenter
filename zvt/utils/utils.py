# -*- coding: utf-8 -*-
import logging
from decimal import getcontext, Decimal
from enum import Enum

import pandas as pd

from zvt.utils.time_utils import to_time_str

getcontext().prec = 16

logger = logging.getLogger(__name__)

none_values = ['不变', '--', '-', '新进']
zero_values = ['不变', '--', '-', '新进']


def to_float(the_str, default=None):
    if not the_str:
        return default
    if the_str in none_values:
        return None

    if '%' in the_str:
        return pct_to_float(the_str)
    try:
        scale = 1.0
        if the_str[-2:] == '万亿':
            the_str = the_str[0:-2]
            scale = 1000000000000
        elif the_str[-1] == '亿':
            the_str = the_str[0:-1]
            scale = 100000000
        elif the_str[-1] == '万':
            the_str = the_str[0:-1]
            scale = 10000
        if not the_str:
            return default
        return float(Decimal(the_str.replace(',', '')) * Decimal(scale))
    except Exception as e:
        logger.error('the_str:{}'.format(the_str))
        logger.exception(e)
        return default


def pct_to_float(the_str, default=None):
    if the_str in none_values:
        return None

    try:
        return float(Decimal(the_str.replace('%', '')) / Decimal(100))
    except Exception as e:
        logger.exception(e)
        return default


def json_callback_param(the_str):
    json_str = the_str[the_str.index("(") + 1:the_str.index(")")].replace('null', 'None')
    return eval(json_str)


def fill_domain_from_dict(the_domain, the_dict: dict, the_map: dict = None, default_func=lambda x: x):
    """
    use field map and related func to fill properties from the dict to the domain


    :param the_domain:
    :type the_domain: DeclarativeMeta
    :param the_dict:
    :type the_dict: dict
    :param the_map:
    :type the_map: dict
    :param default_func:
    :type default_func: function
    """
    if not the_map:
        the_map = {}
        for k in the_dict:
            the_map[k] = (k, default_func)

    for k, v in the_map.items():
        if isinstance(v, tuple):
            field_in_dict = v[0]
            the_func = v[1]
        else:
            field_in_dict = v
            the_func = default_func

        the_value = the_dict.get(field_in_dict)
        if the_value is not None:
            to_value = the_value
            if to_value in none_values:
                setattr(the_domain, k, None)
            else:
                result_value = the_func(to_value)
                setattr(the_domain, k, result_value)
                exec('the_domain.{}=result_value'.format(k))


def marshal_object_for_ui(object):
    if isinstance(object, Enum):
        return object.value

    if isinstance(object, pd.Timestamp):
        return to_time_str(object)

    return object


def chrome_copy_header_to_dict(src):
    lines = src.split('\n')
    header = {}
    if lines:
        for line in lines:
            if len(line) > 0:
                try:
                    index = line.index(':')
                    key = line[:index]
                    value = line[index + 1:]
                    if key and value:
                        header.setdefault(key.strip(), value.strip())
                except Exception:
                    pass
    return header


def add_to_map_list(the_map, key, value):
    result = []
    if key in the_map:
        result = the_map[key]
    else:
        the_map[key] = result

    if value not in result:
        result.append(value)


# the __all__ is generated
__all__ = ['to_float', 'pct_to_float', 'json_callback_param', 'fill_domain_from_dict',
           'marshal_object_for_ui', 'chrome_copy_header_to_dict', 'add_to_map_list']
