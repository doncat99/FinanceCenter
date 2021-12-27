# -*- coding: utf-8 -*-
# this file is generated by gen_kdata_schema function, dont't change it
from sqlalchemy.ext.declarative import declarative_base

from findy.database.schema.datatype import StockKdataCommon

fmKdataBase = declarative_base()


class Stock5mKdata(fmKdataBase, StockKdataCommon):
    __tablename__ = 'stock_5m_kdata'


class Stock5mHfqKdata(fmKdataBase, StockKdataCommon):
    __tablename__ = 'stock_5m_hfq_kdata'


class Stock5mBfqKdata(fmKdataBase, StockKdataCommon):
    __tablename__ = 'stock_5m_bfq_kdata'
