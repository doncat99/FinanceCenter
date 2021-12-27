# -*- coding: utf-8 -*-
import time

from findy.interface import Region, Provider, EntityType
from findy.database.schema.meta.stock_meta import StockDetail
from findy.database.plugins.recorder import RecorderForEntities
from findy.database.quote import get_entities
from findy.vendor.yfinance import YH


class YahooUsStockDetailRecorder(RecorderForEntities):
    region = Region.US
    provider = Provider.Yahoo
    data_schema = StockDetail

    def __init__(self, batch_size=10, force_update=False, sleeping_time=5, codes=None, share_para=None) -> None:
        super().__init__(entity_type=EntityType.StockDetail, batch_size=batch_size, force_update=force_update, sleeping_time=sleeping_time, codes=codes, share_para=share_para)

    async def init_entities(self, db_session):
        # init the entity list
        if not self.force_update:
            self.entities, column_names = get_entities(
                region=self.region,
                provider=self.provider,
                db_session=db_session,
                entity_type=EntityType.StockDetail,
                codes=self.codes,
                filters=[StockDetail.profile.is_(None)])

    def yh_get_info(self, code):
        try:
            return YH.Ticker(code).info
        except Exception as e:
            self.logger.error(f'yh_get_info, code: {code}, error: {e}')
        return None

    async def eval(self, entity, http_session, db_session):
        return not isinstance(entity, StockDetail), 0, None

    async def record(self, entity, http_session, db_session, para):
        start_point = time.time()

        # get stock info
        info = self.yh_get_info(entity.code)

        if info is None or len(info) == 0:
            return True, time.time() - start_point, None

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

        return False, time.time() - start_point, None

    async def persist(self, entity, http_session, db_session, para):
        start_point = time.time()
        db_session.commit()
        return True, time.time() - start_point, 1

    async def on_finish(self):
        pass
