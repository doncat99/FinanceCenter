# -*- coding: utf-8 -*-
# this file is generated by gen_kdata_schema function, dont't change it
from sqlalchemy.ext.declarative import declarative_base

from findy.database.schema import EtfKdataCommon

EtfKdataBase = declarative_base()


class Etf1dKdata(EtfKdataBase, EtfKdataCommon):
    __tablename__ = 'etf_1d_kdata'
