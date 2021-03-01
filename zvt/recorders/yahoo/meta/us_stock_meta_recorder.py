# -*- coding: utf-8 -*-
from zvt.api.data_type import Region, Provider, EntityType
from zvt.domain.meta.stock_meta import StockDetail, Stock
from zvt.contract.recorder import RecorderForEntities
from zvt.contract.api import get_entities
from zvt.recorders.exchange.us_stock_list_spider import ExchangeUsStockListRecorder


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
        super().__init__(batch_size, force_update, sleeping_time)

        # get list at first
        # EastmoneyChinaStockListRecorder().run()

        self.codes = codes
        self.share_para = share_para

        if not self.force_update:
            assert self.region is not None
            self.entities = get_entities(region=self.region,
                                         entity_type=EntityType.StockDetail,
                                         exchanges=['sh', 'sz'],
                                         codes=self.codes,
                                         filters=[StockDetail.profile.is_(None)],
                                         return_type='domain',
                                         provider=self.provider)

    def process_loop(self, entity, http_session):
        assert isinstance(entity, StockDetail)

        self.sleep()


__all__ = ['YahooUsStockListRecorder', 'YahooUsStockDetailRecorder']

if __name__ == '__main__':
    # init_log('china_stock_meta.log')

    # recorder = EastmoneyChinaStockDetailRecorder()
    # recorder.run()
    StockDetail.record_data(codes=['A'], provider=Provider.Yahoo)
