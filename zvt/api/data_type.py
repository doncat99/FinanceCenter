# -*- coding: utf-8 -*-
import enum


class Region(enum.Enum):
    CHN = 'chn'
    US = 'us'
    HK = 'hk'


class Provider(enum.Enum):
    ZVT = 'zvt'
    JoinQuant = 'joinquant'
    BaoStock = 'baostock'
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


class RunMode(enum.Enum):
    Async = 0
    Sync = 1


class Bean(object):

    def __init__(self) -> None:
        super().__init__()
        self.__dict__

    def dict(self):
        return self.__dict__

    def from_dct(self, dct: dict):
        if dct:
            for k in dct:
                self.__dict__[k] = dct[k]
