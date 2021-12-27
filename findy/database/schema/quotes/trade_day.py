# -*- coding: utf-8 -*-
from sqlalchemy.ext.declarative import declarative_base

from findy.database.schema.datatype import Mixin

TradeDayBase = declarative_base()


class StockTradeDay(TradeDayBase, Mixin):
    __tablename__ = 'stock_trade_day'
