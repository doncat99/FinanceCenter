# -*- coding: utf-8 -*-
# this file is generated by gen_kdata_schema function, dont't change it
from sqlalchemy.ext.declarative import declarative_base

from findy.database.schema import BlockKdataCommon

KdataBase = declarative_base()


class Block1dKdata(KdataBase, BlockKdataCommon):
    __tablename__ = 'block_1d_kdata'
