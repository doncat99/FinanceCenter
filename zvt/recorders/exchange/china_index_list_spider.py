# -*- coding: utf-8 -*-
import io

import demjson
import pandas as pd

from zvt.api.data_type import Region, Provider, EntityType
from zvt.api.quote import china_stock_code_to_id
from zvt.domain import IndexStock, Index
from zvt.contract.recorder import Recorder
from zvt.contract.api import df_to_db
from zvt.networking.request import get_http_session, sync_get
from zvt.utils.time_utils import to_pd_timestamp, now_pd_timestamp


class ChinaIndexListSpider(Recorder):
    data_schema = IndexStock

    region = Region.CHN

    def __init__(self, region: Region, batch_size=10, force_update=False, sleeping_time=2.0, provider: Provider = Provider.Exchange) -> None:
        self.region = region
        self.provider = provider
        super().__init__(batch_size, force_update, sleeping_time)

    def run(self):
        http_session = get_http_session(self.mode)

        # 上证、中证
        self.fetch_csi_index(http_session)

        # 深证
        self.fetch_szse_index(http_session)

        # 国证
        # FIXME:已不可用
        # self.fetch_cni_index(http_session)

    def fetch_csi_index(self, http_session) -> None:
        """
        抓取上证、中证指数列表
        """
        url = 'http://www.csindex.com.cn/zh-CN/indices/index' \
              '?page={}&page_size={}&data_type=json&class_1=1&class_2=2&class_7=7&class_10=10'

        index_list = []
        page = 1
        page_size = 50

        while True:
            query_url = url.format(page, page_size)
            text = sync_get(http_session, query_url, return_type='text')
            if text is None:
                continue

            response_dict = demjson.decode(text)
            response_index_list = response_dict.get('list', [])

            if len(response_index_list) == 0:
                break

            index_list.extend(response_index_list)

            self.logger.info(f'上证、中证指数第 {page} 页抓取完成...')
            page += 1
            self.sleep()

        df = pd.DataFrame(index_list)
        df = df[['base_date', 'base_point', 'index_code', 'indx_sname', 'online_date', 'class_eseries']].copy()
        df.columns = ['timestamp', 'base_point', 'code', 'name', 'list_date', 'class_eseries']
        df['category'] = df['class_eseries'].apply(lambda x: x.split(' ')[0].lower())
        df = df.drop('class_eseries', axis=1)
        df = df.loc[df['code'].str.contains(r'^\d{6}$')]

        self.persist_index(df)
        self.logger.info('上证、中证指数列表抓取完成...')

        # 抓取上证、中证指数成分股
        self.fetch_csi_index_component(df, http_session)
        self.logger.info('上证、中证指数成分股抓取完成...')

    def fetch_csi_index_component(self, df: pd.DataFrame, http_session):
        """
        抓取上证、中证指数成分股
        """
        query_url = 'http://www.csindex.com.cn/uploads/file/autofile/cons/{}cons.xls'

        for _, index in df.iterrows():
            index_code = index['code']
            url = query_url.format(index_code)
            content = sync_get(http_session, url, return_type='content')
            if content is None:
                continue

            response_df = pd.read_excel(io.BytesIO(content))

            response_df = response_df[['成分券代码Constituent Code', '成分券名称Constituent Name']].rename(
                columns={'成分券代码Constituent Code': 'stock_code',
                         '成分券名称Constituent Name': 'stock_name'})

            index_id = f'index_cn_{index_code}'
            response_df['entity_id'] = index_id
            response_df['entity_type'] = EntityType.Index.value
            response_df['exchange'] = 'cn'
            response_df['code'] = index_code
            response_df['name'] = index['name']
            response_df['timestamp'] = now_pd_timestamp(self.region)

            response_df['stock_id'] = response_df['stock_code'].apply(lambda x: china_stock_code_to_id(str(x)))
            response_df['id'] = response_df['stock_id'].apply(
                lambda x: f'{index_id}_{x}')

            df_to_db(df=response_df, ref_df=None, region=self.region, data_schema=self.data_schema, provider=self.provider)
            self.logger.info(f'{index["name"]} - {index_code} 成分股抓取完成...')

            self.sleep()

    def fetch_szse_index(self, http_session) -> None:
        """
        抓取深证指数列表
        """
        url = 'http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1812_zs&TABKEY=tab1'
        content = sync_get(http_session, url, return_type='content')
        if content is None:
            return

        df = pd.read_excel(io.BytesIO(content), dtype='str')
        df.columns = ['code', 'name', 'timestamp', 'base_point', 'list_date']
        df['category'] = 'szse'
        df = df.loc[df['code'].str.contains(r'^\d{6}$')]
        self.persist_index(df)
        self.logger.info('深证指数列表抓取完成...')

        # 抓取深证指数成分股
        self.fetch_szse_index_component(df, http_session)
        self.logger.info('深证指数成分股抓取完成...')

    def fetch_szse_index_component(self, df: pd.DataFrame, http_session):
        """
        抓取深证指数成分股
        """
        query_url = 'http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1747_zs&TABKEY=tab1&ZSDM={}'

        for _, index in df.iterrows():
            index_code = index['code']

            url = query_url.format(index_code)
            content = sync_get(http_session, url, return_type='content')
            if content is None:
                continue

            response_df = pd.read_excel(io.BytesIO(content), dtype='str')

            index_id = f'index_cn_{index_code}'
            response_df['entity_id'] = index_id
            response_df['entity_type'] = EntityType.Index.value
            response_df['exchange'] = 'cn'
            response_df['code'] = index_code
            response_df['name'] = index['name']
            response_df['timestamp'] = now_pd_timestamp(self.region)

            response_df.rename(columns={'证券代码': 'stock_code', '证券简称': 'stock_name'}, inplace=True)
            response_df['stock_id'] = response_df['stock_code'].apply(lambda x: china_stock_code_to_id(str(x)))

            response_df['id'] = response_df['stock_id'].apply(
                lambda x: f'{index_id}_{x}')

            df_to_db(df=response_df, ref_df=None, region=self.region, data_schema=self.data_schema, provider=self.provider)
            self.logger.info(f'{index["name"]} - {index_code} 成分股抓取完成...')

            self.sleep()

    def fetch_cni_index(self, http_session) -> None:
        """
        抓取国证指数列表
        """
        url = 'http://www.cnindex.com.cn/zstx/jcxl/'
        text = sync_get(http_session, url, return_type='text')
        if text is None:
            return

        dfs = pd.read_html(text)

        # 第 9 个 table 之后为非股票指数
        dfs = dfs[1:9]

        result_df = pd.DataFrame()
        for df in dfs:
            header = df.iloc[0]
            df = df[1:]
            df.columns = header
            df.astype('str')

            result_df = pd.concat([result_df, df])

        result_df = result_df.drop('样本股数量', axis=1)
        result_df.columns = ['name', 'code', 'timestamp', 'base_point', 'list_date']
        result_df['timestamp'] = result_df['timestamp'].apply(lambda x: x.replace('-', ''))
        result_df['list_date'] = result_df['list_date'].apply(lambda x: x.replace('-', ''))
        result_df['category'] = 'csi'
        result_df = result_df.loc[result_df['code'].str.contains(r'^\d{6}$')]

        self.persist_index(result_df)
        self.logger.info('国证指数列表抓取完成...')

        # 抓取国证指数成分股
        self.fetch_cni_index_component(result_df, http_session)
        self.logger.info('国证指数成分股抓取完成...')

    def fetch_cni_index_component(self, df: pd.DataFrame, http_session):
        """
        抓取国证指数成分股
        """
        query_url = 'http://www.cnindex.com.cn/docs/yb_{}.xls'

        for _, index in df.iterrows():
            index_code = index['code']

            url = query_url.format(index_code)
            content = sync_get(http_session, url, return_type='content')
            if content is None:
                continue

            response_df = pd.read_excel(io.BytesIO(content), dtype='str')

            index_id = f'index_cn_{index_code}'

            try:
                response_df = response_df[['样本股代码']]
            except KeyError:
                response_df = response_df[['证券代码']]

            response_df['entity_id'] = index_id
            response_df['entity_type'] = EntityType.Index.value
            response_df['exchange'] = 'cn'
            response_df['code'] = index_code
            response_df['name'] = index['name']
            response_df['timestamp'] = now_pd_timestamp(Region.CHN)

            response_df.columns = ['stock_code']
            response_df['stock_id'] = response_df['stock_code'].apply(lambda x: china_stock_code_to_id(str(x)))
            response_df['id'] = response_df['stock_id'].apply(
                lambda x: f'{index_id}_{x}')

            df_to_db(df=response_df, ref_df=None, region=self.region, data_schema=self.data_schema, provider=self.provider)
            self.logger.info(f'{index["name"]} - {index_code} 成分股抓取完成...')

            self.sleep()

    def persist_index(self, df) -> None:
        df['timestamp'] = df['timestamp'].apply(lambda x: to_pd_timestamp(x))
        df['list_date'] = df['list_date'].apply(lambda x: to_pd_timestamp(x))
        df['id'] = df['code'].apply(lambda code: f'index_cn_{code}')
        df['entity_id'] = df['id']
        df['exchange'] = 'cn'
        df['entity_type'] = EntityType.Index.value

        df = df.dropna(axis=0, how='any')
        df = df.drop_duplicates(subset='id', keep='last')

        df_to_db(df=df, ref_df=None, region=self.region, data_schema=Index, provider=self.provider)


__all__ = ['ChinaIndexListSpider']

if __name__ == '__main__':
    spider = ChinaIndexListSpider(region=Region.CHN, provider=Provider.Exchange)
    spider.run()
