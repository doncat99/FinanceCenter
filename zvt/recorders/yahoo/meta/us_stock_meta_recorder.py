# -*- coding: utf-8 -*-
from zvt.api.data_type import Region, Provider, EntityType
from zvt.domain.meta.stock_meta import StockDetail, Stock
from zvt.contract.recorder import RecorderForEntities
from zvt.contract.api import get_entities, get_db_session
from zvt.recorders.exchange.us_stock_list_spider import ExchangeUsStockListRecorder
from zvt.networking.request import yh_get_info


class YahooUsStockListRecorder(ExchangeUsStockListRecorder):
    region = Region.US
    provider = Provider.Yahoo
    data_schema = Stock

    def __init__(self, batch_size=10, force_update=False, sleeping_time=5, share_para=None) -> None:
        super().__init__(batch_size, force_update, sleeping_time)


class YahooUsStockDetailRecorder(RecorderForEntities):
    region = Region.US
    provider = Provider.Yahoo
    data_schema = StockDetail

    def __init__(self, batch_size=10, force_update=False, sleeping_time=5, codes=None, share_para=None) -> None:
        exchanges = ['nyse', 'nasdaq', 'amex']

        if not force_update:
            assert self.region is not None
            self.entities = get_entities(region=self.region,
                                         entity_type=EntityType.StockDetail,
                                         exchanges=exchanges,
                                         codes=codes,
                                         filters=[StockDetail.profile.is_(None)],
                                         return_type='domain',
                                         provider=self.provider)

        super().__init__(entity_type=EntityType.StockDetail, exchanges=exchanges, batch_size=batch_size, force_update=force_update, sleeping_time=sleeping_time, codes=codes, share_para=share_para)

    def process_loop(self, entity, http_session):
        assert isinstance(entity, StockDetail)

        # get stock info
        info = yh_get_info(entity.code)

        if info is None or len(info) == 0:
            return None

        if not entity.sector:
            entity.sector = info.get('sector', None)

        if not entity.industry:
            entity.industry = info.get('industry', None)

        if not entity.market_cap or entity.market_cap == 0:
            entity.market_cap = info.get('market_cap', 0)

        entity.profile = info.get('longBusinessSummary', None)
        entity.state = info.get('state', None)
        entity.city = info.get('city', None)
        entity.zip_code = info.get('zip', None)

        entity.last_sale = info.get('previousClose', None)

        session = get_db_session(region=self.region,
                                 provider=self.provider,
                                 data_schema=self.data_schema)
        session.commit()

        self.sleep()


__all__ = ['YahooUsStockListRecorder', 'YahooUsStockDetailRecorder']

if __name__ == '__main__':
    # init_log('china_stock_meta.log')

    # recorder = EastmoneyChinaStockDetailRecorder()
    # recorder.run()
    StockDetail.record_data(codes=['A'], provider=Provider.Yahoo)
