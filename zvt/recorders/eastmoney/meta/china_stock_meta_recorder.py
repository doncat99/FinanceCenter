# -*- coding: utf-8 -*-
import time

from zvt import zvt_config
from zvt.api.data_type import Region, Provider, EntityType
from zvt.domain.meta.stock_meta import StockDetail, Stock
from zvt.contract.recorder import RecorderForEntities
from zvt.contract.api import get_entities, get_db_session
from zvt.recorders.exchange.china_stock_list_spider import ExchangeChinaStockListRecorder
from zvt.networking.request import sync_post
from zvt.utils.time_utils import to_pd_timestamp
from zvt.utils.utils import to_float, pct_to_float


class EastmoneyChinaStockListRecorder(ExchangeChinaStockListRecorder):
    region = Region.CHN
    provider = Provider.EastMoney
    data_schema = Stock


class EastmoneyChinaStockDetailRecorder(RecorderForEntities):
    region = Region.CHN
    provider = Provider.EastMoney
    data_schema = StockDetail

    def __init__(self, batch_size=10, force_update=False, sleeping_time=5, codes=None, share_para=None) -> None:
        if not force_update:
            assert self.region is not None
            self.entities = get_entities(region=self.region,
                                         entity_type=EntityType.StockDetail,
                                         exchanges=['sh', 'sz'],
                                         codes=codes,
                                         filters=[StockDetail.profile.is_(None)],
                                         return_type='domain',
                                         provider=self.provider)

        super().__init__(batch_size=batch_size, force_update=force_update, sleeping_time=sleeping_time, codes=codes, share_para=share_para)

    def process_loop(self, entity, http_session):
        assert isinstance(entity, StockDetail)

        step1 = time.time()
        precision_str = '{' + ':>{},.{}f'.format(8, 4) + '}'

        self.result = None

        if entity.exchange == 'sh':
            fc = "{}01".format(entity.code)
        if entity.exchange == 'sz':
            fc = "{}02".format(entity.code)

        # 基本资料
        param = {"color": "w", "fc": fc, "SecurityCode": "SZ300059"}
        url = 'https://emh5.eastmoney.com/api/GongSiGaiKuang/GetJiBenZiLiao'
        json_result = sync_post(http_session, url, json=param)
        if json_result is None:
            return

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
        entity.area = resp_json['Provice']

        self.sleep()

        # 发行相关
        param = {"color": "w", "fc": fc}
        url = 'https://emh5.eastmoney.com/api/GongSiGaiKuang/GetFaXingXiangGuan'
        json_result = sync_post(http_session, url, json=param)
        if json_result is None:
            return

        resp_json = json_result['FaXingXiangGuan']

        entity.issue_pe = to_float(resp_json['PEIssued'])
        entity.price = to_float(resp_json['IssuePrice'])
        entity.issues = to_float(resp_json['ShareIssued'])
        entity.raising_fund = to_float((resp_json['NetCollection']))
        entity.net_winning_rate = pct_to_float(resp_json['LotRateOn'])

        session = get_db_session(region=self.region,
                                 provider=self.provider,
                                 data_schema=self.data_schema)
        session.commit()

        cost = precision_str.format(time.time() - step1)

        prefix = "finish~ " if zvt_config['debug'] else ""
        postfix = "\n" if zvt_config['debug'] else ""

        if self.result is not None:
            self.logger.info("{}{}, {}, time: {}, size: {:>7,}, date: [ {}, {} ]{}".format(
                prefix, self.data_schema.__name__, entity.id, cost,
                self.result[0], self.result[1], self.result[2], postfix))
        else:
            self.logger.info("{}{}, {}, time: {}{}".format(
                prefix, self.data_schema.__name__, entity.id, cost, postfix))

    def on_finish(self):
        pass


__all__ = ['EastmoneyChinaStockListRecorder', 'EastmoneyChinaStockDetailRecorder']

if __name__ == '__main__':
    # init_log('china_stock_meta.log')

    # recorder = EastmoneyChinaStockDetailRecorder()
    # recorder.run()
    StockDetail.record_data(codes=['000338', '000777'], provider=Provider.EastMoney)
