# -*- coding: utf-8 -*-

from sqlalchemy import Column, String, DateTime, BigInteger, Float
from sqlalchemy.ext.declarative import declarative_base

from zvt.contract import EntityMixin
from zvt.contract.register import register_schema, register_entity
from zvt.contract.common import Region, Provider, EntityType

StockMetaBase = declarative_base()


class BaseSecurity(EntityMixin):
    # 上市日
    list_date = Column(DateTime)
    # 退市日
    end_date = Column(DateTime)


class BasePortfolio(BaseSecurity):
    @classmethod
    def get_stocks(cls, timestamp, code=None, codes=None, ids=None, provider: Provider=Provider.Default):
        """

        :param code: portfolio(etf/block/index...) code
        :param codes: portfolio(etf/block/index...) codes
        :param timestamp:
        :param provider:
        :return:
        """
        portfolio_stock: BasePortfolioStock = eval(f'{cls.__name__}Stock')
        return portfolio_stock.query_data(provider=provider, code=code, codes=codes, ids=ids)


# 个股
@register_entity(entity_type=EntityType.Stock)
class Stock(StockMetaBase, BaseSecurity):
    __tablename__ = EntityType.Stock.value


# 板块
@register_entity(entity_type=EntityType.Block)
class Block(StockMetaBase, BasePortfolio):
    __tablename__ = EntityType.Block.value

    # 板块类型，行业(industry),概念(concept)
    category = Column(String(length=64))


# 指数
@register_entity(entity_type=EntityType.Index)
class Index(StockMetaBase, BasePortfolio):
    __tablename__ = EntityType.Index.value

    # 发布商
    publisher = Column(String(length=64))
    # 类别
    category = Column(String(length=64))
    # 基准点数
    base_point = Column(Float)


# etf
@register_entity(entity_type=EntityType.ETF)
class Etf(StockMetaBase, BasePortfolio):
    __tablename__ = EntityType.ETF.value
    category = Column(String(length=64))

    @classmethod
    def get_stocks(cls, timestamp, code=None, codes=None, ids=None, provider: Provider=Provider.Default):
        from zvt.api.quote import get_etf_stocks
        return get_etf_stocks(code=code, codes=codes, ids=ids, timestamp=timestamp, provider=provider)


# 组合(Etf,Index,Block)和个股(Stock)的关系 应该继承自该类
# 该基础类可以这样理解:
# entity为组合本身,其包含了stock这种entity,timestamp为持仓日期,从py的"你知道你在干啥"的哲学出发，不加任何约束
class BasePortfolioStock(EntityMixin):
    stock_id = Column(String(length=32))
    stock_code = Column(String(length=64))
    stock_name = Column(String(length=128))


# 支持时间变化,报告期标的调整
class BasePortfolioStockHistory(BasePortfolioStock):
    # 报告期,season1,half_year,season3,year
    report_period = Column(String(length=32))
    # 3-31,6-30,9-30,12-31
    report_date = Column(DateTime)

    # 占净值比例
    proportion = Column(Float)
    # 持有股票的数量
    shares = Column(Float)
    # 持有股票的市值
    market_cap = Column(Float)


class BlockStock(StockMetaBase, BasePortfolioStock):
    __tablename__ = 'block_stock'


class IndexStock(StockMetaBase, BasePortfolioStockHistory):
    __tablename__ = 'index_stock'


class EtfStock(StockMetaBase, BasePortfolioStockHistory):
    __tablename__ = 'etf_stock'


# 个股详情
@register_entity(entity_type=EntityType.StockDetail)
class StockDetail(StockMetaBase, BaseSecurity):
    __tablename__ = EntityType.StockDetail.value

    industry = Column(String(length=64))
    sector = Column(String(length=64))
    industry_indices = Column(String(length=64))
    concept_indices = Column(String(length=256))
    area_indices = Column(String(length=64))

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


register_schema(regions=[Region.CHN, Region.US], 
                providers=[Provider.JoinQuant, Provider.EastMoney, Provider.Exchange, 
                           Provider.Sina, Provider.Yahoo], 
                db_name='stock_meta',
                schema_base=StockMetaBase)

__all__ = ['Stock', 'Index', 'Block', 'Etf', 'IndexStock', 'BlockStock', 'EtfStock', 'StockDetail']
