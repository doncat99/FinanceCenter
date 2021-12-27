# -*- coding: utf-8 -*-
from sqlalchemy import Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base

from findy.interface import Region, Provider
from findy.database.schema.datatype import Portfolio, PortfolioStockHistory
from findy.database.schema.register import register_entity

FundMetaBase = declarative_base()


# 个股
@register_entity(entity_type='fund')
class Fund(FundMetaBase, Portfolio):
    __tablename__ = 'fund'
    # 基金管理人
    advisor = Column(String(length=100))
    # 基金托管人
    trustee = Column(String(length=100))

    # 编码    基金运作方式
    # 401001    开放式基金
    # 401002    封闭式基金
    # 401003    QDII
    # 401004    FOF
    # 401005    ETF
    # 401006    LOF
    # 基金运作方式编码
    operate_mode_id = Column(Integer)
    # 基金运作方式
    operate_mode = Column(String(length=32))

    # 编码    基金类别
    # 402001    股票型
    # 402002    货币型
    # 402003    债券型
    # 402004    混合型
    # 402005    基金型
    # 402006    贵金属
    # 402007    封闭式
    # 投资标的类型编码
    underlying_asset_type_id = Column(Integer)
    # 投资标的类型
    underlying_asset_type = Column(String(length=32))

    @classmethod
    async def get_stocks(cls, region: Region, provider: Provider, timestamp, code=None, codes=None, ids=None):
        from findy.database.quote import get_fund_stocks
        return await get_fund_stocks(region=region, provider=provider, timestamp=timestamp, code=code, codes=codes, ids=ids)


class FundStock(FundMetaBase, PortfolioStockHistory):
    __tablename__ = 'fund_stock'
