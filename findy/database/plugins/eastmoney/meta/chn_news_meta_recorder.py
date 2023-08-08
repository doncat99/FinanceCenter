# -*- coding: utf-8 -*-
import logging
import os
import json

import pandas as pd
import numpy as np
from lxml import etree
from sqlalchemy.sql import exists

from findy import findy_config, findy_env
from findy.interface import Region, Provider, EntityType
from findy.database.schema.meta.news_meta import NewsTitle, NewsContent
from findy.database.recorder import RecorderForEntities
from findy.database.persist import df_to_db, to_postgresql
from findy.database.quote import get_entities
from findy.database.data_sources.news.eastmoney_streaming import Eastmoney_Streaming
from findy.utils.functool import time_it
from findy.utils.time import now_pd_timestamp

logger = logging.getLogger(__name__)


class EastMoneyChnNewsTitleRecorder(RecorderForEntities):
    region = Region.CHN
    provider = Provider.EastMoney
    data_schema = NewsTitle

    def __init__(self, batch_size=10, force_update=False, sleep_time=5, codes=None, share_para=None) -> None:
        super().__init__(entity_type=EntityType.News, batch_size=batch_size, force_update=force_update, sleep_time=sleep_time, codes=codes, share_para=share_para)

    async def init_entities(self, db_session):
        df = pd.read_csv(os.path.join(findy_env['source_path'], 'fingpt', "hs_300.csv"))
        stock_list = df.SECURITY_CODE.unique()
        stock_list = [str(s).zfill(6) for s in stock_list]
        return stock_list

    @staticmethod
    def generate_domain_id(entity, df):
        index = df['content_link'].str.split(',', expand=True)[2]
        return entity + '_' + index.str.split('.', expand=True)[0]
    
    @staticmethod
    def stop_get_page(entity, src, ref):
        src_id = EastMoneyChnNewsTitleRecorder.generate_domain_id(entity, src)
        return ref.id.isin(src_id).any()

    def get_news_data(self, code, ref_record):
        # print(f"Collecting stock: {code}")

        # Detailed configs can be found here: https://www.kuaidaili.com/usercenter/tps/
        config = {
            "use_proxy": "kuaidaili",
            "max_retry": 5,
            # "proxy_pages": 5,
            "tunnel": findy_config['kuaidaili_proxy_tunnel'],
            "username": findy_config['kuaidaili_proxy_username'],
            "password": findy_config['kuaidaili_proxy_password'],
        }

        # ATTENTION! Should replace this with your results path!
        downloader = Eastmoney_Streaming(config)
        downloader.use_proxy = True
        downloader.download_streaming_stock(code, EastMoneyChnNewsTitleRecorder.stop_get_page, ref_record, rounds=0)
        return downloader.dataframe

    async def get_referenced_saved_record(self, entity, db_session):
        data, column_names = self.data_schema.query_data(
            region=self.region,
            provider=self.provider,
            db_session=db_session,
            entity_id=entity,
            columns=['id'])
        return pd.DataFrame(data, columns=column_names)

    @time_it
    async def eval(self, entity, http_session, db_session):
        ref_record = await self.get_referenced_saved_record(entity, db_session)
        return False, ref_record

    @time_it
    async def record(self, entity, http_session, db_session, para):
        ref_record = para

        # get news info
        df = self.get_news_data(entity, ref_record)

        if df is None or len(df) == 0:
            return True, None

        df.rename(columns={"read amount": "read_amount", "content link": "content_link",
                             "create time": "create_time",}, inplace=True)
        # df = df.convert_dtypes()
        df['id'] = self.generate_domain_id(entity, df)
        df['entity_id'] = entity
        df['timestamp'] = now_pd_timestamp(Region.CHN)

        return False, df

    @time_it
    async def persist(self, entity, http_session, db_session, df_record):
        # persist to Stock
        saved = await df_to_db(region=self.region,
                               provider=self.provider,
                               data_schema=self.data_schema,
                               db_session=db_session,
                               df=df_record)

        return True, saved

    @time_it
    async def on_finish_entity(self, entity, http_session, db_session, result):
        pass

    async def on_finish(self, entities):
        pass


class EastMoneyChnNewsContentRecorder(RecorderForEntities):
    region = Region.CHN
    provider = Provider.EastMoney
    data_schema = NewsContent
    link_base = "https://guba.eastmoney.com"

    def __init__(self, batch_size=10, force_update=False, sleep_time=5, codes=None, share_para=None) -> None:
        super().__init__(entity_type=EntityType.News, batch_size=batch_size, force_update=force_update, sleep_time=sleep_time, codes=codes, share_para=share_para)

    async def init_entities(self, db_session):
        # init the entity list
        entities, column_names = get_entities(
            region=Region.CHN,
            provider=Provider.EastMoney,
            entity_schema=NewsTitle,
            db_session=db_session,
            filters=[~exists().where(NewsTitle.id==NewsContent.id)])
        return entities

    async def get_one_content(self, http_session, url):
        url = EastMoneyChnNewsContentRecorder.link_base + url

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/112.0",
            "Referer": "https://guba.eastmoney.com/",
        }

        tunnel = findy_config['kuaidaili_proxy_tunnel']
        username = findy_config['kuaidaili_proxy_username']
        password = findy_config['kuaidaili_proxy_password']
        proxies = {
            "http": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password, "proxy": tunnel},
            "https": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password, "proxy": tunnel}
        }

        USE_PROXY = False

        try:
            proxies = proxies["https"] if USE_PROXY else ''
            async with http_session.get(url, headers=headers, proxy=proxies) as response:
                text = await response.text()
                res = etree.HTML(text)
                res_df_part = res.xpath("//script[2]//text()")
                if len(res_df_part) > 0:
                    res = res_df_part[0]
                    res = json.loads(res[17:])
                    return pd.json_normalize(res)

            # requests.DEFAULT_RETRIES = 5
            # s = requests.session()
            # s.keep_alive = False  # close connection when finished
            # proxies = proxies if USE_PROXY else {}
            # response = requests.get(url=url, headers=headers, proxies=proxies)
            # if response.status_code == 200:
            #     res = etree.HTML(response.text)
            #     res_df_part = res.xpath("//script[2]//text()")
            #     if len(res_df_part) > 0:
            #         res = res_df_part[0]
            #         res = json.loads(res[17:])
            #         return pd.json_normalize(res)
        except Exception as e:
            logger.warning(f'news meta exception: {e}')

        return None

    @time_it
    async def eval(self, entity, http_session, db_session):
        return False, None

    @time_it
    async def record(self, entity, http_session, db_session, para):
        ref_record = para

        df = await self.get_one_content(http_session, entity.content_link)

        if df is None or len(df) == 0:
            return True, None

        set_columns = ['post_user', 'post_guba', 'post_publish_time', 'post_last_time',
            'post_display_time', 'post_ip', 'post_checkState', 'post_click_count',
            'post_forward_count', 'post_comment_count', 'post_comment_authority',
            'post_like_count', 'post_is_like', 'post_is_collected', 'post_type',
            'post_source_id', 'post_top_status', 'post_status', 'post_from',
            'post_from_num', 'post_pdf_url', 'post_has_pic',
            'has_pic_not_include_content', 'post_pic_url', 'source_post_id',
            'source_post_state', 'source_post_user_id', 'source_post_user_nickname',
            'source_post_user_type', 'source_post_user_is_majia',
            'source_post_pic_url', 'source_post_title', 'source_post_content',
            'source_post_abstract', 'source_post_ip', 'source_post_type',
            'source_post_guba', 'post_video_url', 'source_post_video_url',
            'source_post_source_id', 'code_name', 'product_type', 'v_user_code',
            'source_click_count', 'source_comment_count', 'source_forward_count',
            'source_publish_time', 'source_user_is_majia', 'ask_chairman_state',
            'selected_post_code', 'selected_post_name', 'selected_relate_guba',
            'ask_question', 'ask_answer', 'qa', 'fp_code', 'codepost_count',
            'extend', 'post_pic_url2', 'source_post_pic_url2', 'relate_topic',
            'source_extend', 'digest_type', 'source_post_atuser',
            'post_inshare_count', 'repost_state', 'post_atuser', 'reptile_state',
            'post_add_list', 'extend_version', 'post_add_time', 'post_modules',
            'post_speccolumn', 'post_ip_address', 'source_post_ip_address',
            'post_mod_time', 'post_mod_count', 'allow_likes_state',
            'system_comment_authority', 'limit_reply_user_auth', 'post_id',
            'post_title', 'post_content', 'post_abstract', 'post_state']

        diff_columns = list(np.setdiff1d(df.columns, set_columns))
        if len(diff_columns) > 0:
            df.drop(columns=diff_columns, inplace=True)

        df = df.convert_dtypes()
        df['id'] = entity.id
        df['entity_id'] = entity.entity_id
        df['timestamp'] = now_pd_timestamp(Region.CHN)
        df['read_amount'] = entity.read_amount
        df['comments'] = entity.comments
        df['title'] = entity.title
        df['content_link'] = entity.content_link
        df['author'] = entity.author

        return False, df

    @time_it
    async def persist(self, entity, http_session, db_session, df_record):
        # persist to Stock
        # saved = await df_to_db(region=self.region,
        #                        provider=self.provider,
        #                        data_schema=self.data_schema,
        #                        db_session=db_session,
        #                        df=df_record,
        #                        drop_duplicates=False)
        
        saved = to_postgresql(self.region, df_record, self.data_schema.__tablename__)

        return True, saved

    @time_it
    async def on_finish_entity(self, entity, http_session, db_session, result):
        pass

    async def on_finish(self, entities):
        pass
