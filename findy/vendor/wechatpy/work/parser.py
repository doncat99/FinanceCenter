# -*- coding: utf-8 -*-


import xmltodict

from findy.vendor.wechatpy.work.events import EVENT_TYPES
from findy.vendor.wechatpy.work.messages import MESSAGE_TYPES
from findy.vendor.wechatpy.messages import UnknownMessage
from findy.vendor.wechatpy.utils import to_text


def parse_message(xml):
    if not xml:
        return
    message = xmltodict.parse(to_text(xml))["xml"]
    message_type = message["MsgType"].lower()
    if message_type == "event":
        event_type = message["Event"].lower()
        message_class = EVENT_TYPES.get(event_type, UnknownMessage)
    else:
        message_class = MESSAGE_TYPES.get(message_type, UnknownMessage)
    return message_class(message)
