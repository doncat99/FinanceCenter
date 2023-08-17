# -*- coding: utf-8 -*-
from sqlalchemy import Column, String, DateTime, BigInteger, Float, Boolean
from sqlalchemy.ext.declarative import declarative_base

from findy.interface import EntityType
from findy.database.schema.datatype import Mixin
from findy.database.schema.register import register_entity

NewsMetaBase = declarative_base()


# 新闻
@register_entity(entity_type=EntityType.News)
class NewsTitle(NewsMetaBase, Mixin):
    __tablename__ = 'news_title'

    read_amount = Column(String)
    comments = Column(String)
    title = Column(String)
    content_link = Column(String)
    author = Column(String)


@register_entity(entity_type=EntityType.News)
class NewsContent(NewsMetaBase, Mixin):
    __tablename__ = 'news_content'
    
    read_amount = Column(String)
    comments = Column(String)
    title = Column(String)
    content_link = Column(String)
    author = Column(String)
    
    post_id = Column(String, default="")
    post_user = Column(String, default="")
    post_guba = Column(String, default="")
    post_title = Column(String, default="")
    post_content = Column(String, default="")
    post_abstract = Column(String, default="")
    post_publish_time = Column(DateTime)
    post_last_time = Column(DateTime)
    post_display_time = Column(DateTime)
    post_ip = Column(String, default="")
    pct_change = Column(Float, default=0.0)
    post_state = Column(BigInteger, default=0)
    post_checkState = Column(BigInteger, default=0)
    post_click_count = Column(BigInteger, default=0)
    post_forward_count = Column(BigInteger, default=0)
    post_comment_count = Column(BigInteger, default=0)
    post_comment_authority = Column(BigInteger, default=0)
    post_like_count = Column(BigInteger, default=0)
    post_is_like = Column(Boolean)
    post_is_collected = Column(Boolean)
    post_type = Column(BigInteger, default=0)
    post_source_id = Column(String, default="")
    post_top_status = Column(BigInteger, default=0)
    post_status = Column(BigInteger, default=0)
    post_from = Column(String, default="")
    post_from_num = Column(BigInteger, default=0)
    post_pdf_url = Column(String, default="")
    post_has_pic = Column(Boolean)
    has_pic_not_include_content = Column(Boolean)
    post_pic_url = Column(String, default="")
    source_post_id = Column(BigInteger, default=0)
    source_post_state = Column(BigInteger, default=0)
    source_post_user_id = Column(String, default="")
    source_post_user_nickname = Column(String, default="")
    source_post_user_type = Column(BigInteger, default=0)
    source_post_user_is_majia = Column(Boolean)
    source_post_pic_url = Column(String, default="")
    source_post_title = Column(String, default="")
    source_post_content = Column(String, default="")
    source_post_abstract = Column(String, default="")
    source_post_ip = Column(String, default="")
    source_post_type = Column(BigInteger, default=0)
    source_post_guba = Column(String, default="")
    post_video_url = Column(String, default="")
    source_post_video_url = Column(String, default="")
    source_post_source_id = Column(String, default="")
    code_name = Column(String, default="")
    product_type = Column(BigInteger, default=0)
    v_user_code = Column(BigInteger, default=0)
    source_click_count = Column(BigInteger, default=0)
    source_comment_count = Column(BigInteger, default=0)
    source_forward_count = Column(BigInteger, default=0)
    source_publish_time = Column(DateTime)
    source_user_is_majia = Column(Boolean)
    ask_chairman_state = Column(String, default="")
    selected_post_code = Column(String, default="")
    selected_post_name = Column(String, default="")
    selected_relate_guba = Column(String, default="")
    ask_question = Column(String, default="")
    ask_answer = Column(String, default="")
    qa = Column(String, default="")
    fp_code = Column(String, default="")
    codepost_count = Column(BigInteger, default=0)
    extend = Column(String, default="")
    post_pic_url2 = Column(String, default="")
    source_post_pic_url2 = Column(String, default="")
    relate_topic = Column(String, default="")
    source_extend = Column(String, default="")
    digest_type = Column(BigInteger, default=0)
    source_post_atuser = Column(String, default="")
    post_inshare_count = Column(BigInteger, default=0)
    repost_state = Column(BigInteger, default=0)
    post_atuser = Column(String, default="")
    reptile_state = Column(BigInteger, default=0)
    post_add_list = Column(String, default="")
    extend_version = Column(BigInteger, default=0)
    post_add_time = Column(String, default="")
    post_modules = Column(String, default="")
    post_speccolumn = Column(String, default="")
    post_ip_address = Column(String, default="")
    source_post_ip_address = Column(String, default="")
    post_mod_time = Column(String, default="")
    post_mod_count = Column(BigInteger, default=0)
    allow_likes_state = Column(BigInteger, default=0)
    system_comment_authority = Column(BigInteger, default=0)
    limit_reply_user_auth = Column(BigInteger, default=0)