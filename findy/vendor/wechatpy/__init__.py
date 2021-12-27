# -*- coding: utf-8 -*-
import logging

from findy.vendor.wechatpy.client import WeChatClient  # NOQA
from findy.vendor.wechatpy.component import ComponentOAuth, WeChatComponent  # NOQA
from findy.vendor.wechatpy.exceptions import (
    WeChatClientException,
    WeChatException,
    WeChatOAuthException,
    WeChatPayException,
)  # NOQA
from findy.vendor.wechatpy.oauth import WeChatOAuth  # NOQA
from findy.vendor.wechatpy.parser import parse_message  # NOQA
from findy.vendor.wechatpy.pay import WeChatPay  # NOQA
from findy.vendor.wechatpy.replies import create_reply  # NOQA

__version__ = "2.0.0.alpha11"
__author__ = "messense"

# Set default logging handler to avoid "No handler found" warnings.
logging.getLogger(__name__).addHandler(logging.NullHandler())
