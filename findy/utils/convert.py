# -*- coding: utf-8 -*-
import logging
from decimal import Decimal

logger = logging.getLogger(__name__)

none_values = ['不变', '--', '-', '新进']


def pct_to_float(the_str, default=None):
    if the_str in none_values:
        return None

    try:
        return float(Decimal(the_str.replace('%', '')) / Decimal(100))
    except Exception as e:
        logger.exception(e)
        return default


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
        logger.error(f'the_str:{the_str}')
        logger.exception(e)
        return default


def json_callback_param(the_str):
    json_str = the_str[the_str.index("(") + 1:the_str.index(")")].replace('null', 'None')
    return eval(json_str)
