# -*- coding: utf-8 -*-
import pandas as pd

from findy.interface import Region, Provider
from findy.interface.writer import df_to_db
from findy.database.schema.meta.stock_meta import Index
from findy.database.context import get_db_session
from findy.utils.pd import pd_valid
from findy.utils.time import to_pd_timestamp

CHINA_STOCK_MAIN_INDEX = [{'id': 'index_sh_000001',
                           'entity_id': 'index_sh_000001',
                           'code': '000001',
                           'name': '上证指数',
                           'timestamp': '1990-12-19',
                           'exchange': 'sh',
                           'entity_type': 'index',
                           'category': 'main'},
                          {'id': 'index_sh_000016',
                           'entity_id': 'index_sh_000016',
                           'code': '000016',
                           'name': '上证50',
                           'timestamp': '2004-01-02',
                           'exchange': 'sh',
                           'entity_type': 'index',
                           'category': 'main'},
                          {'id': 'index_sh_000905',
                           'entity_id': 'index_sh_000905',
                           'code': '000905',
                           'name': '中证500',
                           'timestamp': '2005-01-04',
                           'exchange': 'sh',
                           'entity_type': 'index',
                           'category': 'main'},
                          {'id': 'index_sz_399001',
                           'entity_id': 'index_sz_399001',
                           'code': '399001',
                           'name': '深证成指',
                           'timestamp': '1991-04-03',
                           'exchange': 'sz',
                           'entity_type': 'index',
                           'category': 'main'},
                          {'id': 'index_sz_399106',
                           'entity_id': 'index_sz_399106',
                           'code': '399106',
                           'name': '深证综指',
                           'timestamp': '1991-04-03',
                           'exchange': 'sz',
                           'entity_type': 'index',
                           'category': 'main'},
                          {'id': 'index_sz_399300',
                           'entity_id': 'index_sz_399300',
                           'code': '399300',
                           'name': '沪深300',
                           'timestamp': '2002-01-04',
                           'exchange': 'sz',
                           'entity_type': 'index',
                           'category': 'main'},
                          {'id': 'index_sz_399005',
                           'entity_id': 'index_sz_399005',
                           'code': '399005',
                           'name': '中小板指',
                           'timestamp': '2006-01-24',
                           'exchange': 'sz',
                           'entity_type': 'index',
                           'category': 'main'},
                          {'id': 'index_sz_399006',
                           'entity_id': 'index_sz_399006',
                           'code': '399006',
                           'name': '创业板指',
                           'timestamp': '2010-06-01',
                           'exchange': 'sz',
                           'entity_type': 'index',
                           'category': 'main'},
                          {'id': 'index_sh_000688',
                           'entity_id': 'index_sh_000688',
                           'code': '000688',
                           'name': '科创50',
                           'timestamp': '2019-01-01',
                           'exchange': 'sh',
                           'entity_type': 'index',
                           'category': 'main'},
                          # # 聚宽编码
                          # # 市场通编码    市场通名称
                          # # 310001    沪股通
                          # # 310002    深股通
                          # # 310003    港股通（沪）
                          # # 310004    港股通（深）
                          {'id': 'index_sz_310001',
                           'entity_id': 'index_sz_310001',
                           'code': '310001',
                           'name': '沪股通',
                           'timestamp': '2014-11-17',
                           'exchange': 'sz',
                           'entity_type': 'index',
                           'category': 'main'},
                          {'id': 'index_sz_310002',
                           'entity_id': 'index_sz_310002',
                           'code': '310002',
                           'name': '深股通',
                           'timestamp': '2014-11-17',
                           'exchange': 'sz',
                           'entity_type': 'index',
                           'category': 'main'},
                          {'id': 'index_sz_310003',
                           'entity_id': 'index_sz_310003',
                           'code': '310003',
                           'name': '港股通（沪）',
                           'timestamp': '2014-11-17',
                           'exchange': 'sz',
                           'entity_type': 'index',
                           'category': 'main'},
                          {'id': 'index_sz_310004',
                           'entity_id': 'index_sz_310004',
                           'code': '310004',
                           'name': '港股通（深）',
                           'timestamp': '2014-11-17',
                           'exchange': 'sz',
                           'entity_type': 'index',
                           'category': 'main'}
                          ]

US_STOCK_MAIN_INDEX = [{'id': 'index_cme_SPY',
                        'entity_id': 'index_cme_SPY',
                        'code': 'SPY',
                        'name': "Standard & Poor's 500",
                        'timestamp': '1990-12-19',
                        'exchange': 'cme',
                        'entity_type': 'index',
                        'category': 'main'},
                       {'id': 'index_cme_^DJI',
                        'entity_id': 'index_cme_^DJI',
                        'code': '^DJI',
                        'name': "Dow Jones Industrial Average",
                        'timestamp': '1990-12-19',
                        'exchange': 'cme',
                        'entity_type': 'index',
                        'category': 'main'},
                       ]


async def init_main_index(region: Region, provider=Provider.Exchange):
    if region == Region.CHN:
        for item in CHINA_STOCK_MAIN_INDEX:
            item['timestamp'] = to_pd_timestamp(item['timestamp'])
        df = pd.DataFrame(CHINA_STOCK_MAIN_INDEX)
    elif region == Region.US:
        for item in US_STOCK_MAIN_INDEX:
            item['timestamp'] = to_pd_timestamp(item['timestamp'])
        df = pd.DataFrame(US_STOCK_MAIN_INDEX)
    else:
        print("index not initialized, in file: init_main_index")
        df = pd.DataFrame()

    if pd_valid(df):
        await df_to_db(region=region,
                       provider=provider,
                       data_schema=Index,
                       db_session=get_db_session(region, provider, Index),
                       df=df)
