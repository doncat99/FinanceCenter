# -*- coding: utf-8 -*-
from sqlalchemy import Column, String, DateTime, BigInteger, Float
from sqlalchemy.ext.declarative import declarative_base

from findy.interface import Region, Provider, EntityType
from findy.database.schema.datatype import EntityMixin, Portfolio, PortfolioStock, PortfolioStockHistory
from findy.database.schema.register import register_entity

StockMetaBase = declarative_base()


# 个股
@register_entity(entity_type=EntityType.Stock)
class Stock(StockMetaBase, EntityMixin):
    __tablename__ = EntityType.Stock.value


# 板块
@register_entity(entity_type=EntityType.Block)
class Block(StockMetaBase, Portfolio):
    __tablename__ = EntityType.Block.value

    # 板块类型，行业(industry),概念(concept)
    category = Column(String(length=64))


# 指数
@register_entity(entity_type=EntityType.Index)
class Index(StockMetaBase, Portfolio):
    __tablename__ = EntityType.Index.value

    # 发布商
    publisher = Column(String(length=64))
    # 类别
    category = Column(String(length=64))
    # 基准点数
    base_point = Column(Float)


# etf
@register_entity(entity_type=EntityType.ETF)
class Etf(StockMetaBase, Portfolio):
    __tablename__ = EntityType.ETF.value
    category = Column(String(length=64))

    @classmethod
    async def get_stocks(cls, region: Region, provider: Provider, timestamp, code=None, codes=None, ids=None):
        from findy.database.quote import get_etf_stocks
        return await get_etf_stocks(region=region, code=code, codes=codes, ids=ids, timestamp=timestamp, provider=provider)


class BlockStock(StockMetaBase, PortfolioStock):
    __tablename__ = 'block_stock'


class IndexStock(StockMetaBase, PortfolioStockHistory):
    __tablename__ = 'index_stock'


class EtfStock(StockMetaBase, PortfolioStockHistory):
    __tablename__ = 'etf_stock'


# 个股详情
@register_entity(entity_type=EntityType.StockDetail)
class StockDetail(StockMetaBase, EntityMixin):
    __tablename__ = EntityType.StockDetail.value

    name = Column(String)
    industry = Column(String)
    sector = Column(String)
    country = Column(String)
    state = Column(String)
    city = Column(String)

    zip_code = Column(String)
    phone = Column(String)

    market_cap = Column(Float)
    last_sale = Column(Float)
    volume = Column(Float)
    net_change = Column(Float)
    pct_change = Column(Float)
    url = Column(String)

    chairman = Column(String)
    manager = Column(String)
    secretary = Column(String)

    # 成立日期
    date_of_establishment = Column(DateTime)
    # 公司简介
    profile = Column(String(length=4096))
    # 主营业务
    main_business = Column(String(length=1024))
    # 发行量(股)
    issues = Column(BigInteger)
    # 发行价格
    price = Column(Float)
    # 募资净额(元)
    raising_fund = Column(Float)
    # 发行市盈率
    issue_pe = Column(Float)
    # 网上中签率
    net_winning_rate = Column(Float)

    # 员工数
    fulltime_employees = Column(Float)
    # 网站
    website = Column(String)
