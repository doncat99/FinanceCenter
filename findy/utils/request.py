# -*- coding: utf-8 -*-
import logging
from http import client
from retrying import retry

import requests
from aiohttp import ClientSession, ClientTimeout, TCPConnector
# from aiohttp_client_cache import CachedSession, SQLiteBackend

# from findy.interface import RunMode
from findy.utils.cache import hashable_lru

logger = logging.getLogger(__name__)

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


def get_http_session():
    # if fetch_mode == RunMode.Sync:
    #     http_session = TimeoutRequestsSession()
    #     http_session.mount('http://', requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=0))
    #     http_session.mount('https://', requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=0))
    #     return http_session

    timeout = ClientTimeout(total=None,
                            connect=None,
                            sock_connect=None,
                            sock_read=None)
    # cache = SQLiteBackend(
    #     cache_name='~/.cache/aiohttp-requests.db',        # For SQLite, this will be used as the filename
    #     expire_after=24,                                  # By default, cached responses expire in a day
    #     # expire_after_urls={'*.site.com/static': 24 * 7},  # Requests with this pattern will expire in a week
    #     # ignored_params=['auth_token'],                    # Ignore this param when caching responses
    # )
    return ClientSession(connector=TCPConnector(limit=30, limit_per_host=10, ttl_dns_cache=50,
                                                ssl=False, force_close=True, enable_cleanup_closed=True),
                         trust_env=True,
                        #  headers={"Connection": "close"},
                         timeout=timeout
                         )


@hashable_lru
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
            pass
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


@hashable_lru
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
