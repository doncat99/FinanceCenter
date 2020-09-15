# -*- coding: utf-8 -*-
import multiprocessing

from zvt.contract.recorder import Recorder
from zvt.contract.common import Provider, EntityType
from zvt.utils.time_utils import to_pd_timestamp
from zvt.utils.utils import to_float, pct_to_float
from zvt.utils.request_utils import get_http_session, request_post
from zvt.contract.api import get_entities
from zvt.domain.meta.stock_meta import StockDetail, Stock
from zvt.recorders.exchange.us_stock_list_spider import ExchangeUsStockListRecorder

from tqdm import tqdm


class YahooUsStockListRecorder(ExchangeUsStockListRecorder):
    data_schema = Stock
    provider = Provider.Yahoo

    def __init__(self, batch_size=10, force_update=False, sleeping_time=5, share_para=None) -> None:
        super().__init__(batch_size, force_update, sleeping_time)


class YahooUsStockDetailRecorder(Recorder):
    provider = Provider.Yahoo
    data_schema = StockDetail

    def __init__(self, batch_size=10, force_update=False, sleeping_time=5, codes=None, share_para=None) -> None:
        super().__init__(batch_size, force_update, sleeping_time)

        # get list at first
        # EastmoneyChinaStockListRecorder().run()

        self.codes = codes
        self.share_para = share_para
        
        if not self.force_update:
            self.entities = get_entities(session=self.session,
                                         entity_type=EntityType.StockDetail.value,
                                         exchanges=['sh', 'sz'],
                                         codes=self.codes,
                                         filters=[StockDetail.profile.is_(None)],
                                         return_type='domain',
                                         provider=self.provider)

    def run(self):
        process_identity = multiprocessing.current_process()._identity
        if len(process_identity) > 0:
            #  The worker process tqdm bar shall start at Position 1
            worker_id = (process_identity[0]-1)%self.share_para[1] + 1
        else:
            worker_id = 0
        desc = "{:02d} : {}".format(worker_id, self.share_para[0])

        with tqdm(total=len(self.entities), ncols=80, position=worker_id, desc=desc, leave=self.share_para[3]) as pbar:
            http_session = get_http_session()
            
            for security_item in self.entities:
                assert isinstance(security_item, StockDetail)

                # if security_item.exchange == 'sh':
                #     fc = "{}01".format(security_item.code)
                # if security_item.exchange == 'sz':
                #     fc = "{}02".format(security_item.code)

                # # 基本资料
                # param = {"color": "w", "fc": fc, "SecurityCode": "SZ300059"}
                # resp =  request_post(http_session, 'https://emh5.eastmoney.com/api/GongSiGaiKuang/GetJiBenZiLiao', json=param)
                # resp.encoding = 'utf8'

                # resp_json = resp.json()['Result']['JiBenZiLiao']

                # security_item.profile = resp_json['CompRofile']
                # security_item.main_business = resp_json['MainBusiness']
                # security_item.date_of_establishment = to_pd_timestamp(resp_json['FoundDate'])

                # # 关联行业
                # industry = ','.join(resp_json['Industry'].split('-'))
                # security_item.industry = industry

                # # 关联概念
                # security_item.concept_indices = resp_json['Block']

                # # 关联地区
                # security_item.area_indices = resp_json['Provice']

                # # 发行相关
                # param = {"color": "w", "fc": fc}
                # resp = request_post(http_session, 'https://emh5.eastmoney.com/api/GongSiGaiKuang/GetFaXingXiangGuan', json=param)
                # resp.encoding = 'utf8'

                # resp_json = resp.json()['Result']['FaXingXiangGuan']

                # security_item.issue_pe = to_float(resp_json['PEIssued'])
                # security_item.price = to_float(resp_json['IssuePrice'])
                # security_item.issues = to_float(resp_json['ShareIssued'])
                # security_item.raising_fund = to_float((resp_json['NetCollection']))
                # security_item.net_winning_rate = pct_to_float(resp_json['LotRateOn'])
                
                # self.session.commit()
                # self.logger.info('finish recording stock meta for: {}'.format(security_item.code))

                self.share_para[2].acquire()
                pbar.update()
                self.share_para[2].release()
                
                self.sleep()


__all__ = ['YahooUsStockListRecorder', 'YahooUsStockDetailRecorder']

if __name__ == '__main__':
    # init_log('china_stock_meta.log')

    # recorder = EastmoneyChinaStockDetailRecorder()
    # recorder.run()
    StockDetail.record_data(codes=['A'], provider=Provider.Yahoo)
