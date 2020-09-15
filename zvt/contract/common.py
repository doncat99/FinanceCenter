# -*- coding: utf-8 -*-
import enum


class Region(enum.Enum):
    CHN = 'chn'
    US = 'us'


class Provider(enum.Enum):
    ZVT = 'zvt'
    JoinQuant = 'joinquant'
    EastMoney = 'eastmoney'
    Exchange = 'exchange'
    Sina = 'sina'
    Yahoo = 'yahoo'
    Default = None

class EntityType(enum.Enum):
    Stock = 'stock'
    Block = 'block'
    Coin = 'coin'
    Index = 'index'
    ETF = 'etf'
    StockDetail = 'stock_detail'