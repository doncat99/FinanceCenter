# -*- coding: utf-8 -*-
import time

import pandas as pd
# from numba import njit

from findy import findy_config
from findy.interface import Region, Provider, EntityType
from findy.interface.writer import df_to_db
from findy.database.schema import BlockCategory
from findy.database.schema.meta.stock_meta import Block, BlockStock
from findy.database.plugins.recorder import RecorderForEntities, TimeSeriesDataRecorder
from findy.database.quote import china_stock_code_to_id
from findy.utils.time import now_pd_timestamp, PD_TIME_FORMAT_DAY
from findy.utils.convert import json_callback_param


class EastmoneyChinaBlockRecorder(RecorderForEntities):
    region = Region.CHN
    provider = Provider.EastMoney
    data_schema = Block

    # 用于抓取行业/概念/地域列表
    category_map_url = {
        BlockCategory.industry: 'https://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?type=CT&cmd=C._BKHY&sty=DCRRBKCPAL&st=(ChangePercent)&sr=-1&p=1&ps=200&lvl=&cb=jsonp_F1A61014DE5E45B7A50068EA290BC918&token=4f1862fc3b5e77c150a2b985b12db0fd&_=08766',
        BlockCategory.concept: 'https://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?type=CT&cmd=C._BKGN&sty=DCRRBKCPAL&st=(ChangePercent)&sr=-1&p=1&ps=300&lvl=&cb=jsonp_3071689CC1E6486A80027D69E8B33F26&token=4f1862fc3b5e77c150a2b985b12db0fd&_=08251',
        # BlockCategory.area: 'https://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?type=CT&cmd=C._BKDY&sty=DCRRBKCPAL&st=(ChangePercent)&sr=-1&p=1&ps=200&lvl=&cb=jsonp_A597D4867B3D4659A203AADE5B3B3AD5&token=4f1862fc3b5e77c150a2b985b12db0fd&_=02443'
    }

    async def init_entities(self, db_session):
        self.entities = [BlockCategory.industry, BlockCategory.concept]

    async def process_loop(self, entity, http_session, db_session, throttler):
        async with throttler:
            async with http_session.get(self.category_map_url[entity]) as response:
                text = await response.text()
                if text is None:
                    return {}

                results = json_callback_param(text)

                # @njit(nopython=True)
                def numba_boost_up(results):
                    the_list = []
                    for result in results:
                        items = result.split(',')
                        code = items[1]
                        name = items[2]
                        entity_id = f'block_cn_{code}'
                        the_list.append({
                            'id': entity_id,
                            'entity_id': entity_id,
                            'entity_type': EntityType.Block.value,
                            'exchange': 'cn',
                            'code': code,
                            'name': name,
                            'category': entity.value
                        })
                    return the_list

                the_list = numba_boost_up(results)
                if the_list:
                    df = pd.DataFrame.from_records(the_list)
                    await df_to_db(region=self.region,
                                   provider=self.provider,
                                   data_schema=self.data_schema,
                                   db_session=db_session,
                                   df=df)
                self.logger.info(f"finish record sina blocks:{entity.value}")


class EastmoneyChinaBlockStockRecorder(TimeSeriesDataRecorder):
    region = Region.CHN
    provider = Provider.EastMoney
    entity_schema = Block
    data_schema = BlockStock

    # 用于抓取行业包含的股票
    category_stocks_url = 'https://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx?type=CT&cmd=C.{}{}&sty=SFCOO&st=(Close)&sr=-1&p=1&ps=300&cb=jsonp_B66B5BAA1C1B47B5BB9778045845B947&token=7bc05d0d4c3c22ef9fca8c2a912d779c'

    def __init__(self, exchanges=None, entity_ids=None, codes=None, batch_size=10, force_update=False, sleeping_time=5,
                 default_size=findy_config['batch_size'], real_time=False, fix_duplicate_way='add',
                 start_timestamp=None, end_timestamp=None, close_hour=0, close_minute=0) -> None:
        super().__init__(EntityType.Block, exchanges, entity_ids, codes, batch_size, force_update, sleeping_time,
                         default_size, real_time, fix_duplicate_way, start_timestamp, end_timestamp, close_hour,
                         close_minute)

    def generate_domain_id(self, entity, df, time_fmt=PD_TIME_FORMAT_DAY):
        return entity.id + '_' + df['stock_id']

    async def record(self, entity, http_session, db_session, para):
        start_point = time.time()

        (ref_record, start, end, size, timestamps) = para

        url = self.category_stocks_url.format(entity.code, '1')
        async with http_session.get(url) as response:
            text = await response.text()
            if text is None:
                return True, time.time() - start_point, None

            results = json_callback_param(text)

            # @njit(nopython=True)
            def numba_boost_up(results):
                the_list = []
                for result in results:
                    items = result.split(',')
                    stock_code = items[1]
                    stock_id = china_stock_code_to_id(stock_code)
                    the_list.append({
                        'stock_id': stock_id,
                        'stock_code': stock_code,
                        'stock_name': items[2],
                    })
                return the_list

            the_list = numba_boost_up(results)
            if the_list:
                df = pd.DataFrame.from_records(the_list)
                return False, time.time() - start_point, (ref_record, self.format(entity, df))

            return True, time.time() - start_point, None

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
