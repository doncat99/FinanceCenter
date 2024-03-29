# -*- coding: utf-8 -*-
# this file is generated by gen_kdata_schema function, dont't change it
from sqlalchemy.ext.declarative import declarative_base

from findy.database.schema.datatype import StockKdataCommon

mKdataBase = declarative_base()


class Stock1mKdata(mKdataBase, StockKdataCommon):
    __tablename__ = 'stock_1m_kdata'


class Stock1mHfqKdata(mKdataBase, StockKdataCommon):
    __tablename__ = 'stock_1m_hfq_kdata'


class Stock1mBfqKdata(mKdataBase, StockKdataCommon):
    __tablename__ = 'stock_1m_bfq_kdata'
