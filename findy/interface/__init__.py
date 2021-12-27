# -*- coding: utf-8 -*-
import enum

from .constants import *


class Region(enum.Enum):
    CHN = 'chn'
    US = 'us'
    HK = 'hk'


class Provider(enum.Enum):
    Exchange = 'exchange'
    JoinQuant = 'joinquant'
    BaoStock = 'baostock'
    TuShare = 'tushare'
    EastMoney = 'eastmoney'
    Sina = 'sina'
    Yahoo = 'yahoo'
    Findy = 'findy'


class EntityType(enum.Enum):
    Stock = 'stock'
    Block = 'block'
    Coin = 'coin'
    Index = 'index'
    ETF = 'etf'
    StockDetail = 'stock_detail'


class RunMode(enum.Enum):
    Serial = 2
    Parallel = 3


class Signal(enum.Enum):
    HOLD = 0
    BUY = 1
    STRONG_BUY = 2
    SELL = 3
    STRONG_SELL = 4
