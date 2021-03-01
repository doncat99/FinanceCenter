# -*- coding: utf-8 -*-
from sqlalchemy.ext.declarative import declarative_base

from zvt.api.data_type import Region, Provider
from zvt.contract import Mixin
from zvt.contract.register import register_schema

TradeDayBase = declarative_base()


class StockTradeDay(TradeDayBase, Mixin):
    __tablename__ = 'stock_trade_day'


register_schema(regions=[Region.CHN, Region.US],
                providers={Region.CHN: [Provider.JoinQuant, Provider.BaoStock],
                           Region.US: [Provider.Yahoo]},
                db_name='trade_day',
                schema_base=TradeDayBase)


# the __all__ is generated
__all__ = ['StockTradeDay']
