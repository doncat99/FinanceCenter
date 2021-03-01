# -*- coding: utf-8 -*-
import json

import demjson
import pandas as pd
from numba import njit

from zvt import zvt_config
from zvt.api.data_type import Region, Provider, EntityType
from zvt.api.quote import china_stock_code_to_id
from zvt.domain import BlockStock, BlockCategory, Block
from zvt.contract.recorder import RecorderForEntities, TimeSeriesDataRecorder
from zvt.contract.api import df_to_db
from zvt.networking.request import sync_get
from zvt.utils.time_utils import now_pd_timestamp, PD_TIME_FORMAT_DAY


class SinaChinaBlockRecorder(RecorderForEntities):
    region = Region.CHN
    provider = Provider.Sina
    data_schema = Block

    # 用于抓取行业/概念/地域列表
    category_map_url = {
        BlockCategory.industry: 'http://vip.stock.finance.sina.com.cn/q/view/newSinaHy.php',
        BlockCategory.concept: 'http://money.finance.sina.com.cn/q/view/newFLJK.php?param=class'
        # StockCategory.area: 'http://money.finance.sina.com.cn/q/view/newFLJK.php?param=area',
    }

    def init_entities(self):
        self.entities = [(category, url) for category, url in self.category_map_url.items()]

    def process_loop(self, entity, http_session):
        category, url = entity

        text = sync_get(http_session, url, encoding='gbk', return_type='text')
        if text is None:
            return

        json_str = text[text.index('{'):text.index('}') + 1]
        tmp_json = json.loads(json_str)

        @njit(nopython=True)
        def numba_boost_up(tmp_json):
            the_list = []
            for code in tmp_json:
                name = tmp_json[code].split(',')[1]
                entity_id = f'block_cn_{code}'
                the_list.append({
                    'id': entity_id,
                    'entity_id': entity_id,
                    'entity_type': EntityType.Block.value,
                    'exchange': 'cn',
                    'code': code,
                    'name': name,
                    'category': category.value
                })
            return the_list

        the_list = numba_boost_up(tmp_json)
        if the_list:
            df = pd.DataFrame.from_records(the_list)
            df_to_db(df=df, ref_df=None, region=Region.CHN, data_schema=self.data_schema, provider=self.provider)

        self.logger.info(f"finish record sina blocks:{category.value}")


class SinaChinaBlockStockRecorder(TimeSeriesDataRecorder):
    region = Region.CHN
    provider = Provider.Sina
    entity_schema = Block
    data_schema = BlockStock

    # 用于抓取行业包含的股票
    category_stocks_url = 'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page={}&num=5000&sort=symbol&asc=1&node={}&symbol=&_s_r_a=page'

    def __init__(self, entity_type=EntityType.Block, exchanges=None, entity_ids=None,
                 codes=None, batch_size=10, force_update=True, sleeping_time=5,
                 default_size=zvt_config['batch_size'], real_time=False,
                 fix_duplicate_way='add', start_timestamp=None, end_timestamp=None,
                 close_hour=0, close_minute=0) -> None:
        super().__init__(entity_type, exchanges, entity_ids, codes, batch_size,
                         force_update, sleeping_time, default_size, real_time,
                         fix_duplicate_way, start_timestamp, end_timestamp,
                         close_hour, close_minute)

    def generate_domain_id(self, entity, df, time_fmt=PD_TIME_FORMAT_DAY):
        return entity.id + '_' + df['stock_id']

    def record(self, entity, start, end, size, timestamps, http_session):
        for page in range(1, 5):
            text = sync_get(http_session, self.category_stocks_url.format(page, entity.code), return_type='text')
            if text is None or text == 'null':
                break
            category_jsons = demjson.decode(text)

            # @njit(nopython=True)
            def numba_boost_up(category_jsons):
                the_list = []
                for category in category_jsons:
                    stock_code = category['code']
                    stock_id = china_stock_code_to_id(stock_code)
                    the_list.append({
                        'stock_id': stock_id,
                        'stock_code': stock_code,
                        'stock_name': category['name'],
                    })
                return the_list

            the_list = numba_boost_up(category_jsons)

            if the_list:
                return pd.DataFrame.from_records(the_list)
        return None

    def format(self, entity, df):
        df['timestamp'] = now_pd_timestamp(Region.CHN)

        df['entity_id'] = entity.id
        df['provider'] = self.provider.value
        df['code'] = entity.code
        df['name'] = entity.name
        df['level'] = self.level.value
        df['exchange'] = entity.exchange
        df['entity_type'] = EntityType.Block.value

        df['id'] = self.generate_domain_id(entity, df)
        return df


__all__ = ['SinaChinaBlockRecorder', 'SinaChinaBlockStockRecorder']


if __name__ == '__main__':
    # init_log('sina_china_stock_category.log')

    recorder = SinaChinaBlockStockRecorder(codes=['new_cbzz'])
    recorder.run()
