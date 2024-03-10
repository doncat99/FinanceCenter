# -*- coding: utf-8 -*-
import logging

import re
import pandas as pd
from newsdataapi import NewsDataApiClient

from findy import findy_config
from findy.interface import Region, Provider
from findy.database.schema.meta.news_meta import News
from findy.database.recorder import RecorderForEntities
from findy.database.persist import df_to_db
from findy.utils.functool import time_it

logger = logging.getLogger(__name__)


class NewsDataUsNewsRecorder(RecorderForEntities):
    region = Region.US
    provider = Provider.NewsData
    data_schema = News
    max_retry = 3

    async def init_entities(self, db_session):

        # init the entity list
        entities = self.generate_search_keys(self.share_para[4][0], max_keyword_len=512)
        return entities

    @time_it
    async def eval(self, entity, http_session, db_session):
        return False, None

    @time_it
    async def record(self, entity, http_session, db_session, para):
        # Language      Language Code
        # Afrikaans          af
        # Albanian           sq
        # Amharic            am
        # Arabic             ar
        # Armenian           zy
        # Assamese           as
        # Azerbaijani        az
        # Basque             eu
        # Belarusian         be
        # Bengali            bn
        # Bosnian            bs
        # Bulgarian          bg
        # Burmese            my
        # Catalan            ca
        # Central Kurdish    ckb
        # Chinese            zh
        # Croatian           hr
        # Czech              cs
        # Danish             da
        # Dutch              nl
        # English            en
        # Estonian           et
        # Filipino           pi
        # Finnish            fi
        # French             fr
        # Georgian           ka
        # German             de
        # Greek              el
        # Gujarati           gu
        # Hebrew             he
        # Hindi              hi
        # Hungarian          hu
        # Icelandic          is
        # Indonesian         id
        # Italian            it
        # Japanese           jp
        # Kannada            kn
        # Khmer              kh
        # Kinyarwanda        rw
        # Korean             ko
        # Latvian            lv
        # Lithuanian         lt
        # Luxembourgish      lb
        # Macedonian         mk
        # Malay              ms
        # Malayalam          ml
        # Maltese            mt
        # Maori              mi
        # Marathi            mr
        # Mongolian          mn
        # Nepali             ne
        # Norwegian          no
        # Oriya              or
        # Pashto             ps
        # Persian            fa
        # Polish             pl
        # Portuguese         pt
        # Punjabi            pa
        # Romanian           ro
        # Russian            ru
        # Samoan             sm
        # Serbian            sr
        # Shona              sn
        # Sindhi             sd
        # Sinhala            si
        # Slovak             sk
        # Slovenian          sl
        # Somali             so
        # Spanish            es
        # Swahili            sw
        # Swedish            sv
        # Tajik              tg
        # Tamil              ta
        # Telugu             te
        # Thai               th
        # Turkish            tr
        # Turkmen            tk
        # Ukrainian          uk
        # Urdu               ur
        # Uzbek              uz
        # Vietnamese         vi
        # Welsh              cy
        language = 'en'
        api = NewsDataApiClient(apikey=findy_config['newsdata.io'])
        articles_result = self.request_news(api, entity, language)
        df = pd.DataFrame(articles_result)
        df.drop_duplicates(subset='article_id', keep='last', inplace=True)
        df['id'] = df['article_id']
        return False, df

    @time_it
    async def persist(self, entity, http_session, db_session, df_record):
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

    def generate_search_keys(self, keywords, max_keyword_len=128):
        search_list = []
        
        pattern = ' OR '
        q = pattern.join([f'"{keyword}"' for keyword in keywords])

        matchs = [m.start() for m in re.finditer(pattern, q)]
        index_offset = 0
        match_cnt = len(matchs)

        for index, match in enumerate(matchs):
            if index == match_cnt - 1:
                search_list.append(q[index_offset:matchs[index]])

            elif match-index_offset > max_keyword_len:
                anchor = matchs[index - 1]
                search_list.append(q[index_offset : anchor])
                index_offset = anchor + len(pattern)

        return search_list

    def inner_request_news(self, api, keyword, language, page):

        for _ in range(self.max_retry):
            try:
                response = api.news_api(q=keyword, language=language, page=page)
                if response is not None and response['status'] == 'success':
                    break
            except Exception as e:
                logger.warning(f'get newsdata failed: {e}')
                response = None

        return response

    def request_news(self, api, keywords, language, page=None):
        articles_list = []
        totalResults = 0
        
        while True:
            response = self.inner_request_news(api, keywords, language, page)
            
            if response is not None:
                totalResults = response['totalResults']
    
            if totalResults == 0:
                break
            
            articles_list.extend(response['results'])
            
            if response['nextPage'] is None:
                break
            
            page = response['nextPage']

        return articles_list
