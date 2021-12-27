# -*- coding: utf-8 -*-
import time

import demjson

from findy import findy_config
from findy.interface import Region, Provider, EntityType
from findy.database.schema.meta.stock_meta import StockDetail
from findy.database.plugins.recorder import RecorderForEntities
from findy.database.quote import get_entities
from findy.utils.time import PRECISION_STR, to_pd_timestamp
from findy.utils.convert import to_float, pct_to_float


class EastmoneyChinaStockDetailRecorder(RecorderForEntities):
    region = Region.CHN
    provider = Provider.EastMoney
    data_schema = StockDetail

    async def init_entities(self, db_session):
        if not self.force_update:
            # init the entity list
            self.entities, column_names = get_entities(
                region=self.region,
                provider=self.provider,
                db_session=db_session,
                entity_type=EntityType.StockDetail,
                exchanges=['sh', 'sz'],
                codes=self.codes,
                filters=[self.data_schema.profile.is_(None)])

    async def process_loop(self, entity, http_session, db_session, throttler):
        assert isinstance(entity, self.data_schema)

        async with throttler:
            now = time.time()

            self.result = None

            if entity.exchange == 'sh':
                fc = f"{entity.code}01"
            if entity.exchange == 'sz':
                fc = f"{entity.code}02"

            # 基本资料
            params = {"color": "w", "fc": fc, "SecurityCode": "SZ300059"}
            url = 'https://emh5.eastmoney.com/api/GongSiGaiKuang/GetJiBenZiLiao'
            async with http_session.post(url, params=params) as response:
                text = await response.text()
                if text is None:
                    return {}

                json_result = demjson.decode(text)

                resp_json = json_result['JiBenZiLiao']

                entity.profile = resp_json['CompRofile']
                entity.main_business = resp_json['MainBusiness']
                entity.date_of_establishment = to_pd_timestamp(resp_json['FoundDate'])

                # 关联行业
                industry = ','.join(resp_json['Industry'].split('-'))
                entity.industry = industry

                # 关联概念
                entity.sector = resp_json['Block']

                # 关联地区
                entity.state = resp_json['Provice']

            # 发行相关
            params = {"color": "w", "fc": fc}
            url = 'https://emh5.eastmoney.com/api/GongSiGaiKuang/GetFaXingXiangGuan'
            async with http_session.post(url, params=params) as response:
                text = await response.text()
                if text is None:
                    return {}

                json_result = demjson.decode(text)

                resp_json = json_result['FaXingXiangGuan']

                entity.issue_pe = to_float(resp_json['PEIssued'])
                entity.price = to_float(resp_json['IssuePrice'])
                entity.issues = to_float(resp_json['ShareIssued'])
                entity.raising_fund = to_float((resp_json['NetCollection']))
                entity.net_winning_rate = pct_to_float(resp_json['LotRateOn'])

                db_session.commit()

                cost = PRECISION_STR.format(time.time() - now)

                prefix = "finish~ " if findy_config['debug'] else ""
                postfix = "\n" if findy_config['debug'] else ""

                if self.result is not None:
                    self.logger.info("{}{:>14}, {:>18}, time: {}, size: {:>7}, date: [ {}, {} ]{}".format(
                        prefix, self.data_schema.__name__, entity.id, cost,
                        self.result[0], self.result[1], self.result[2], postfix))
                else:
                    self.logger.info("{}{:>14}, {:>18}, time: {}{}".format(
                        prefix, self.data_schema.__name__, entity.id, cost, postfix))

    async def on_finish(self):
        pass
