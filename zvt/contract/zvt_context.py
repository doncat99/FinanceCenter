# -*- coding: utf-8 -*-
import contextlib
import sqlalchemy.exc
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.pool import QueuePool
from sqlalchemy.ext.declarative import declarative_base

from zvt.contract.common import Region
from zvt import zvt_env


# all registered providers
providers = {}

# all registered entity types
entity_types = []

# all registered schemas
schemas = []

# entity_type -> schema
entity_schema_map = {}

# global sessions
sessions = {}

# provider_dbname -> session
db_session_map = {}

# provider -> [db_name1,db_name2...]
provider_map_dbnames = {}

# db_name -> [declarative_base1,declarative_base2...]
dbname_map_base = {}

# db_name -> [declarative_meta1,declarative_meta2...]
dbname_map_schemas = {}

# entity_type -> schema
entity_map_schemas = {}

chn_map_key = [
    "exchange_stock_meta",
    "exchange_overall",
    "eastmoney_trading",
    "eastmoney_stock_meta",
    "eastmoney_stock_detail",
    "eastmoney_holder",
    "eastmoney_finance",
    "eastmoney_dividend_financing",
    "eastmoney_block_1d_kdata",
    "eastmoney_block_1wk_kdata",
    "eastmoney_block_1mon_kdata",
    "joinquant_stock_meta",
    "joinquant_trade_day",
    "joinquant_overall",
    "joinquant_valuation",
    "joinquant_stock_1mon_kdata",
    "joinquant_stock_1mon_hfq_kdata",
    "joinquant_stock_1wk_kdata",
    "joinquant_stock_1wk_hfq_kdata",
    "joinquant_stock_1d_kdata",
    "joinquant_stock_1d_hfq_kdata",
    "joinquant_stock_1h_kdata",
    "joinquant_stock_1h_hfq_kdata",
    "joinquant_stock_30m_kdata",
    "joinquant_stock_30m_hfq_kdata",
    "joinquant_stock_15m_kdata",
    "joinquant_stock_15m_hfq_kdata",
    "joinquant_stock_5m_kdata",
    "joinquant_stock_5m_hfq_kdata",
    "joinquant_stock_1m_kdata",
    "joinquant_stock_1m_hfq_kdata",
    "sina_etf_1d_kdata",
    "sina_index_1d_kdata",
    "sina_money_flow",
    "sina_stock_meta",
    "zvt_trader_info",
]

us_map_key = [
    "exchange_stock_meta",
    "yahoo_stock_meta",
    "yahoo_stock_detail",
    "yahoo_trade_day",
    "yahoo_stock_1mon_kdata",
    "yahoo_stock_1mon_hfq_kdata",
    "yahoo_stock_1wk_kdata",
    "yahoo_stock_1wk_hfq_kdata",
    "yahoo_stock_1d_kdata",
    "yahoo_stock_1d_hfq_kdata",
    "yahoo_stock_1h_kdata",
    "yahoo_stock_1h_hfq_kdata",
    "yahoo_stock_30m_kdata",
    "yahoo_stock_30m_hfq_kdata",
    "yahoo_stock_15m_kdata",
    "yahoo_stock_15m_hfq_kdata",
    "yahoo_stock_5m_kdata",
    "yahoo_stock_5m_hfq_kdata",
    "yahoo_stock_1m_kdata",
    "yahoo_stock_1m_hfq_kdata",
    "zvt_trader_info",
]

def build_map(region: Region, map_key):

    db_name = "{}_{}".format(zvt_env['db_name'], region.value)
    link = 'postgresql+psycopg2://{}:{}@{}/{}'.format(
        zvt_env['db_user'], zvt_env['db_pass'], zvt_env['db_host'], db_name)
    db_engine = create_engine(link,
                              encoding='utf-8',
                              echo=False,
                              poolclass=QueuePool,
                              pool_size=zvt_env['cpus'],
                              pool_timeout=30,
                              pool_pre_ping=True,
                              max_overflow=10)

    with contextlib.suppress(sqlalchemy.exc.ProgrammingError):
        with sqlalchemy.create_engine('postgresql:///postgres', isolation_level='AUTOCOMMIT').connect() as connection:
            cmd = "CREATE DATABASE {}_{}".format(zvt_env['db_name'], region.value)
            connection.execute(cmd)

    dict_ = {}
    for key in map_key:
        new_key = "{}_{}".format(region.value, key)
        dict_.update({new_key: db_engine})
    return dict_

# provider_dbname -> engine
if "db_engine" in zvt_env and zvt_env['db_engine'] == "postgresql":
    chn_map = build_map(Region.CHN, chn_map_key)
    us_map = build_map(Region.US, us_map_key)
    db_engine_map = {**chn_map, **us_map}
else:
    db_engine_map = {}
