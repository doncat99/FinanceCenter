# -*- coding:utf-8 -*-
import zlib
import ctypes
import inspect
import findy.vendor.baostock.data.resultset as rs
import findy.vendor.baostock.common.contants as cons
import findy.vendor.baostock.util.stringutil as strUtil
import findy.vendor.baostock.common.context as conx
import findy.vendor.baostock.util.socketutil as sock
import findy.vendor.baostock.data.messageheader as msgheader


def subscribe_by_code(code_list, subscribe_type=0, fncallback=None, options="", user_params=None):
    """订阅实时行情
    @param code_list: 每只证券代码之间用“英文逗号分隔符”，结尾不存在“英文逗号分隔符”；证券代码格式：sh.600000，前两位为证券市场：“sz”深圳、“sh”上海
    @param subscribe_type: 订阅方式 0：按证券代码订阅， 1：按行情数据类型订阅。0.7.5版本只支持按证券代码订阅；
    @param fncallback: 自定义回调方法
    @param options: 预留参数
    @param user_params: 用户参数，回调时原样返回
    """
    data = rs.SubscibeData()
    data.method = "subscribe_by_code"
    data.options = options
    data.user_params = user_params

    if code_list is None or code_list == "":
        data.error_code = cons.BSERR_CODE_INVALIED
        data.error_msg = "证券代码不正确"
        return data

    # 判断code_list，如果大于限制，则进行截取
    if len(code_list.split(cons.ATTRIBUTE_SPLIT)) > cons.BAOSTOCK_REALTIME_LIMIT_COUNT:
        code_list = cons.ATTRIBUTE_SPLIT.join(code_list.split(cons.ATTRIBUTE_SPLIT)[0:cons.BAOSTOCK_REALTIME_LIMIT_COUNT])

    data.code_list = code_list.lower()

    # 设置订阅方式 默认为0
    # if subscribe_type == 0 or subscribe_type == 1:
    if subscribe_type == 0:
        pass
    else:
        subscribe_type = 0
    data.subscribe_type = subscribe_type

    if not callable(fncallback):
        fncallback = DemoCallback
    data.fncallback = fncallback
    user_id = ""
    try:
        user_id = getattr(conx, "user_id")
    except Exception:
        print("you don't login.")
        data.error_code = cons.BSERR_NO_LOGIN
        data.error_msg = "you don't login."
        return data

    data.user_id = user_id
    param = "%s\1%s\1%s\1%s\1%s\1%s\1%s" % (
        "subscribe_by_code", user_id, subscribe_type,
        code_list, fncallback, options, user_params)
    msg_body = strUtil.organize_realtime_msg_body(param)
    msg_header = msgheader.to_message_header(
        cons.MESSAGE_TYPE_SUBSCRIPTIONS_BY_SECURITYID_REQUEST, len(msg_body))
    data.msg_type = cons.MESSAGE_TYPE_SUBSCRIPTIONS_BY_SECURITYID_REQUEST
    data.msg_body = msg_body

    head_body = msg_header + msg_body
    crc32str = zlib.crc32(bytes(head_body, encoding='utf-8'))

    sock.send_real_time_subscibe(head_body + cons.MESSAGE_SPLIT + str(crc32str), data)

    return data


def cancel_subscribe(ident):
    """取消实时行情
    @param thread: 实时行情的线程；
    """
    data = rs.SubscibeData()

    # 取订阅的子循环
    try:
        _async_raise(ident, SystemExit)
    except (ValueError, TypeError):
        data.error_code = cons.BSERR_PARAM_ERR
        data.error_msg = "取消订阅失败"
        return data

    # 封装待发出的消息
    user_id = ""
    try:
        user_id = getattr(conx, "user_id")
    except Exception:
        print("you don't login.")
        data.error_code = cons.BSERR_NO_LOGIN
        data.error_msg = "you don't login."
        return data

    data.user_id = user_id

    # 组织体信息
    msg_body = "cancel_subscribe" + cons.MESSAGE_SPLIT + user_id

    # 组织头信息
    msg_header = msgheader.to_message_header(
        cons.MESSAGE_TYPE_CANCEL_SUBSCRIBE_REQUEST, len(msg_body))
    head_body = msg_header + msg_body

    crc32str = zlib.crc32(bytes(head_body, encoding='utf-8'))

    # 向服务器发送取消订阅
    sock.send_cancel_real_time_msg(head_body + cons.MESSAGE_SPLIT + str(crc32str))

    data.error_code = cons.BSERR_SUCCESS
    data.error_msg = "success"

#     if receive_data is None or receive_data.strip() == "":
#         data.error_code = cons.BSERR_RECVSOCK_FAIL
#         data.error_msg = "网络接收错误。"
#         return data
# 
#     msg_header = receive_data[0:cons.MESSAGE_HEADER_LENGTH]
#     msg_body = receive_data[cons.MESSAGE_HEADER_LENGTH:-1]
# 
#     header_arr = msg_header.split(cons.MESSAGE_SPLIT)
#     body_arr = msg_body.split(cons.MESSAGE_SPLIT)
# 
#     data.msg_type = header_arr[1]
#     data.msg_body_length = header_arr[2]
# 
#     data.error_code = body_arr[0]
#     data.error_msg = body_arr[1]

    return data


def _async_raise(tid, exctype):
    """raises the exception, performs cleanup if needed"""
    tid = ctypes.c_long(tid)
    if not inspect.isclass(exctype):
        exctype = type(exctype)
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))
    if res == 0:
        raise ValueError("invalid thread id")
    elif res != 1:
        ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
        raise SystemError("PyThreadState_SetAsyncExc failed")


def DemoCallback(quantdata):
    """
    DemoCallback 是订阅时提供的回调函数模板。该函数只有一个为ResultData类型的参数quantdata
    :param quantdata:ResultData
    :return:
    """
    print(quantdata.data)
