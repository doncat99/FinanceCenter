# -*- coding: utf-8 -*-
# this file is generated by gen_kdata_schema function, dont't change it
from sqlalchemy.ext.declarative import declarative_base
from findy.database.schema.datatype import IndexKdataCommon

IndexKdataBase = declarative_base()


class Index1dKdata(IndexKdataBase, IndexKdataCommon):
    __tablename__ = 'index_1d_kdata'
