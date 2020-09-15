# -*- coding: utf-8 -*-
# from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base

from zvt.contract import Mixin
from zvt.contract.register import register_schema
from zvt.contract.common import Region, Provider

TradeDayBase = declarative_base()


class StockTradeDay(TradeDayBase, Mixin):
    __tablename__ = 'stock_trade_day'

    # # 交易所
    # exchange = Column(String(length=32))


register_schema(regions=[Region.CHN, Region.US], 
                providers=[Provider.JoinQuant, Provider.Yahoo], 
                db_name='trade_day', schema_base=TradeDayBase)

__all__ = ['StockTradeDay']
