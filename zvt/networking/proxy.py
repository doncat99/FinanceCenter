# -*- coding: utf-8 -*-
import requests
import json

import pandas as pd


def init_proxy_pool():
    url = 'https://raw.githubusercontent.com/fate0/proxylist/master/proxy.list'

    try:
        resp = requests.get(url)
    except Exception as e:
        print(f'url: {url}, error: {e}')
        return pd.DataFrame()

    proxy_list_string = resp.text.splitlines()

    if len(proxy_list_string) == 0:
        return None, None

    proxy_list_dict = [json.loads(proxy) for proxy in proxy_list_string]
    df = pd.DataFrame.from_dict(proxy_list_dict)

    df_http = df[df["type"] == 'http']
    df_https = df[df["type"] == 'https']

    df_http.reset_index(drop=True, inplace=True)
    df_https.reset_index(drop=True, inplace=True)

    return df_http, df_https


# http_proxies = []
# https_proxies = []


# def init_proxy_pool():
#     global http_proxies
#     global https_proxies

#     url = 'http://127.0.0.1:5010/get_all/'

#     try:
#         logger.info(f'HTTP GET PROXY: {url}')
#         resp = requests.get(url)
#     except Exception as e:
#         print(f'url: {url}, error: {e}')
#         return pd.DataFrame()

#     proxy_list_dict = json.loads(resp.content)

#     for proxy in proxy_list_dict:
#         if proxy['scheme'] == 'http':
#             http_proxies.append(f"http://{proxy['proxy']}/")
#         elif proxy['scheme'] == 'https':
#             https_proxies.append(f"https://{proxy['proxy']}/")


# def get_proxy():
#     return {
#         'http': random.choice(http_proxies) if len(http_proxies) > 0 else '',
#         'https': random.choice(https_proxies) if len(https_proxies) > 0 else '',
#     }