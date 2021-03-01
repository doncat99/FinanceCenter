# -*- coding: utf-8 -*-
import logging
# import random
from http import client
from retrying import retry

# import pandas as pd
import requests
from aiohttp import ClientSession, TCPConnector
import baostock as bs
import jqdatapy.api as jq

from zvt.api.data_type import RunMode


logger = logging.getLogger(__name__)

bs.login()

client.HTTPConnection._http_vsn = 11
client.HTTPConnection._http_vsn_str = 'HTTP/1.1'

http_timeout = (20, 60)
max_retries = 3


class TimeoutRequestsSession(requests.Session):
    def request(self, *args, **kwargs):
        kwargs.setdefault('timeout', http_timeout)
        return super(TimeoutRequestsSession, self).request(*args, **kwargs)


def get_proxy():
    return requests.get("http://127.0.0.1:5010/get/").json()


def retry_if_connection_error(exception):
    """ Specify an exception you need. or just True"""
    logger.debug(f'request exception: {exception}')

    return True
    # return isinstance(exception, ConnectionError)


def get_http_session(fetch_mode: RunMode = RunMode.Sync):
    if fetch_mode == RunMode.Sync:
        http_session = TimeoutRequestsSession()
        http_session.mount('http://', requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=0))
        http_session.mount('https://', requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=0))
        return http_session

    return ClientSession(connector=TCPConnector(limit=64, verify_ssl=False), trust_env=True)


def sync_get(http_session: requests.Session, url, headers=None, encoding='utf-8', params={}, enable_proxy=False, return_type=None):
    @retry(retry_on_exception=retry_if_connection_error, stop_max_attempt_number=max_retries, wait_fixed=2000)
    def _sync_get(http_session: requests.Session, url, enable_proxy, headers=None, encoding='utf-8', params={}):
        proxies = get_proxy() if enable_proxy else None
        logger.debug(f'proxies: {proxies}')
        return http_session.get(url, headers=headers, params=params, proxies=proxies, verify=False)

    def _sync_get_no_proxy(http_session: requests.Session, url, headers=None, encoding='utf-8', params={}):
        try:
            return http_session.get(url, headers=headers, params=params, verify=False)
        except:
            return None

    logger.debug(f'HTTP GET: {url}')

    try:
        resp = _sync_get(http_session, url, enable_proxy=enable_proxy, headers=headers, encoding=encoding, params=params)
    except Exception as e:
        if enable_proxy:
            resp = _sync_get_no_proxy(http_session, url, headers=headers, encoding=encoding, params=params)
            if resp is None:
                logger.error(f'url: {url}, error: {e}')
                return None
        else:
            logger.error(f'url: {url}, error: {e}')
            return None

    resp.encoding = encoding

    if return_type == 'text':
        return resp.text
    elif return_type == 'content':
        return resp.content
    else:
        return resp


def sync_post(http_session: requests.Session, url, json=None, encoding=['utf-8', 'gbk'], enable_proxy=False):
    @retry(retry_on_exception=retry_if_connection_error, stop_max_attempt_number=max_retries, wait_fixed=2000)
    def _sync_post(http_session: requests.Session, url, enable_proxy, json=None, encoding=['utf-8', 'gbk']):
        proxies = get_proxy() if enable_proxy else None
        logger.debug(f'proxies: {proxies}')
        return http_session.post(url=url, json=json, proxies=proxies, verify=False)

    def _sync_post_no_proxy(http_session: requests.Session, url, json=None, encoding=['utf-8', 'gbk']):
        try:
            return http_session.post(url=url, json=json, verify=False)
        except:
            return None

    logger.debug(f'HTTP POST: {url}, json: {json}')

    if json is None:
        return None

    try:
        resp = _sync_post(http_session, url=url, enable_proxy=enable_proxy, json=json)
    except Exception as e:
        if enable_proxy:
            resp = _sync_post_no_proxy(http_session, url, json=json)
            if resp is None:
                logger.error(f'url: {url}, error: {e}')
                return None
        else:
            logger.error(f'url: {url}, error: {e}')
            return None

    for encode in encoding:
        try:
            resp.encoding = encode
            origin_result = resp.json().get('Result')
            if origin_result is not None:
                return origin_result
        except Exception as e:
            logger.error(f'json decode failed, code: {resp.status_code}, codec: {encode}, content: {resp.text}, error: {e}')
    return None


async def async_get(http_session: ClientSession, url, params) -> str:
    """Make http GET request to fetch ticker data."""
    resp = await http_session.get(url, params=params)
    if resp.status != 200:
        logger.error(f'{url} failed, status={resp.status}')
        return resp
    logger.debug(f'Got response [{resp.status}] for URL: {url}')
    tdata = await resp.text()
    return tdata


async def async_post(http_session: ClientSession, url, params) -> str:
    """Make http GET request to fetch ticker data."""
    resp = await http_session.post(url, params=params)
    if resp.status != 200:
        logger.error(f'{url} failed, status={resp.status}')
        return resp
    logger.debug(f'Got response [{resp.status}] for URL: {url}')
    tdata = await resp.text()
    return tdata


def jq_get_fundamentals(table='balance', columns=None, code='000001.XSHE', date=None,
                        count=1000, parse_dates=['day', 'pubDate']):
    try:
        return jq.get_fundamentals(table=table, columns=columns, code=code, date=date, count=count,
                                   parse_dates=parse_dates)
    except Exception as e:
        logger.error(f'jq_get_fundamentals, code: {code}, error: {e}')

    return None


def jq_get_mtss(code='000001.XSHE', date='2005-01-01', end_date=None):
    try:
        return jq.get_mtss(code=code, date=date, end_date=end_date)
    except Exception as e:
        logger.error(f'jq_get_mtss, code: {code}, error: {e}')

    return None


def jq_run_query(table='finance.STK_EXCHANGE_TRADE_INFO', columns=None, conditions=None,
                 count=1000, dtype={'code': str, 'symbol': str}, parse_dates=['day', 'pub_date']):
    try:
        return jq.run_query(table=table, columns=columns, conditions=conditions, count=count,
                            dtype=dtype, parse_dates=parse_dates)
    except Exception as e:
        logger.error(f'jq_run_query, code: {dtype["code"]}, error: {e}')

    return None


def jq_get_all_securities(code='stock', date=None):
    try:
        return jq.get_all_securities(code=code, date=date)
    except Exception as e:
        logger.error(f'jq_get_all_securities, code: {code}, error: {e}')

    return None


def jq_get_trade_days(date='1990-01-01', end_date=None):
    try:
        return jq.get_trade_days(date=date, end_date=end_date)
    except Exception as e:
        logger.error(f'jq_get_trade_days, date: {date}, error: {e}')

    return None


def jq_get_token(mob=None, pwd=None, force=True):
    try:
        return jq.get_token(mob=mob, pwd=pwd, force=force)
    except Exception as e:
        logger.error(f'jq_get_token, mob: {mob}, error: {e}')

    return None


def jq_get_money_flow(code, date, end_date=None):
    try:
        return jq.get_money_flow(code=code, date=date, end_date=end_date)
    except Exception as e:
        logger.error(f'jq_get_money_flow, code: {code}, error: {e}')

    return None


def jq_get_bars(code="600000.XSHG", count=10, unit='1d', end_date=None, fq_ref_date=None,
                return_type='df', parse_dates=['date']):
    try:
        return jq.get_bars(code=code, count=count, unit=unit, end_date=end_date,
                           fq_ref_date=fq_ref_date, return_type=return_type, parse_dates=parse_dates)
    except Exception as e:
        logger.error(f'jq_get_bars, code: {code}, error: {e}')

    return None


def bao_get_trade_days(start_date=None, end_date=None):
    @retry(retry_on_exception=retry_if_connection_error, stop_max_attempt_number=max_retries, wait_fixed=2000)
    def _bao_get_trade_days(start_date=None, end_date=None):
        k_rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)
        return k_rs.get_data()

    logger.info("HTTP GET: trade_days, with start_date={}, end_date={}".format(start_date, end_date))

    try:
        return _bao_get_trade_days(start_date=start_date, end_date=end_date)
    except Exception as e:
        logger.error(f'bao_get_trade_days, error: {e}')

    return None


def bao_get_all_securities(entity_type):
    @retry(retry_on_exception=retry_if_connection_error, stop_max_attempt_number=max_retries, wait_fixed=2000)
    def _bao_get_all_securities(entity_type):
        k_rs = bs.query_stock_basic()
        df = k_rs.get_data()
        return df[df['type'] == entity_type] if not df.empty else df

    try:
        return _bao_get_all_securities(entity_type)
    except Exception as e:
        logger.error(f'bao_get_all_securities, error: {e}')

    return None


def bao_get_bars(code, start, end, frequency="d", adjustflag="3",
                 fields="date, code, open, high, low, close, preclose, volume, amount, adjustflag, turn, tradestatus, pctChg, isST"):
    @retry(retry_on_exception=retry_if_connection_error, stop_max_attempt_number=max_retries, wait_fixed=2000)
    def _bao_get_bars(code, start, end, fields, frequency="d", adjustflag="3"):
        k_rs = bs.query_history_k_data_plus(code, start_date=start, end_date=end, frequency=frequency,
                                            adjustflag=adjustflag, fields=fields)
        return k_rs.get_data()

    logger.debug("HTTP GET: bars, with code={}, unit={}, start={}, end={}".format(code, frequency, start, end))

    try:
        return _bao_get_bars(code, start, end, frequency, adjustflag, fields)
    except Exception as e:
        logger.error(f'bao_get_bars, code: {code}, error: {e}')


def yh_get_bars(code, interval, start=None, end=None, actions=True, enable_proxy=False):
    import yfinance as yf

    @retry(retry_on_exception=retry_if_connection_error, stop_max_attempt_number=max_retries, wait_fixed=2000)
    def _yh_get_bars(code, interval, enable_proxy, start=None, end=None, actions=True):
        proxies = get_proxy() if enable_proxy else None
        logger.debug(f'proxies: {proxies}')
        return yf.Ticker(code).history(interval=interval, start=start, end=end, actions=actions, proxy=proxies, debug=False)

    def _yh_get_bars_no_proxy(code, interval, start=None, end=None, actions=True):
        try:
            return yf.Ticker(code).history(interval=interval, start=start, end=end, actions=actions, debug=False)
        except:
            return None

    logger.debug("HTTP GET: bars, with code={}, unit={}, start={}, end={}".format(code, interval, start, end))

    try:
        return _yh_get_bars(code, interval=interval, enable_proxy=enable_proxy, start=start, end=end, actions=actions)
    except Exception as e:
        if enable_proxy:
            result = _yh_get_bars(code, interval=interval, enable_proxy=False, start=start, end=end, actions=actions)
            if result is not None:
                return result
        logger.error(f'yh_get_bars, code: {code}, error: {e}')

    return None


# def fh_get_bars(client, code, interval, start, end):
#     logger.debug("HTTP GET: bars, with code={}, unit={}, start={}, end={}".format(code, interval, start, end))
#     _from = to_time_int(start)
#     _to = now_timestamp() if end is None else to_time_int(end)
#     res = client.stock_candles(code, interval, _from, _to)
#     df = pd.DataFrame(res)
#     df.rename(columns={'o':'open', 'c':'close', 'h':'high', 'l':'low', 'v':'volume', 't':'timestamp'}, inplace=True)
#     df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
#     df.set_index('timestamp', drop=True, inplace=True)
#     return df
