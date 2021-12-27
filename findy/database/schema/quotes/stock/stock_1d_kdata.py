# -*- coding: utf-8 -*-
# this file is generated by gen_kdata_schema function, dont't change it
from sqlalchemy import Column, Float, Integer
from sqlalchemy.ext.declarative import declarative_base

from findy.database.schema.datatype import StockKdataCommon

dKdataBase = declarative_base()


class Stock1dKdata(dKdataBase, StockKdataCommon):
    __tablename__ = 'stock_1d_kdata'

    pre_close = Column(Float)
    # 滚动市盈率
    pe_ttm = Column(Float)
    # 滚动市销率
    ps_ttm = Column(Float)
    # 滚动市现率
    pcf_ncf_ttm = Column(Float)
    # 市净率
    pb_mrq = Column(Float)
    is_st = Column(Integer)


class Stock1dHfqKdata(dKdataBase, StockKdataCommon):
    __tablename__ = 'stock_1d_hfq_kdata'

    pre_close = Column(Float)
    # 滚动市盈率
    pe_ttm = Column(Float)
    # 滚动市销率
    ps_ttm = Column(Float)
    # 滚动市现率
    pcf_ncf_ttm = Column(Float)
    # 市净率
    pb_mrq = Column(Float)
    is_st = Column(Integer)


class Stock1dBfqKdata(dKdataBase, StockKdataCommon):
    __tablename__ = 'stock_1d_bfq_kdata'

    pre_close = Column(Float)
    # 滚动市盈率
    pe_ttm = Column(Float)
    # 滚动市销率
    ps_ttm = Column(Float)
    # 滚动市现率
    pcf_ncf_ttm = Column(Float)
    # 市净率
    pb_mrq = Column(Float)
    is_st = Column(Integer)