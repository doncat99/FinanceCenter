# -*- coding:utf-8 -*-
"""
获取默认socket
@author: baostock.com
@group : baostock.com
@contact: baostock@163.com
"""
import socket
import threading
import zlib
import findy.vendor.baostock.common.contants as cons
import findy.vendor.baostock.common.context as context

Default_Socket_Size = 65536  # 32768


class SocketUtil(object):
    """Socket工具类"""
    # 记录第一个被创建对象的引用
    instance = None
    # 记录是否执行过初始化动作
    init_flag = False

    def __new__(cls, *args, **kwargs):
        # 1. 判断类属性是否是空对象
        if cls.instance is None:
            # 2. 调用父类的方法，为第一个对象分配空间
            cls.instance = super().__new__(cls)
        # 3. 返回类属性保存的对象引用
        return cls.instance

    def __init__(self):
        SocketUtil.init_flag = True

    def connect(self):
        """创建连接"""
        try:
            mySockect = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            mySockect.connect((cons.BAOSTOCK_SERVER_IP, cons.BAOSTOCK_SERVER_PORT))
        except Exception:
            # print("服务器连接失败，请稍后再试。")
            pass
        setattr(context, "default_socket", mySockect)


def get_default_socket():
    """获取默认连接"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((cons.BAOSTOCK_SERVER_IP, cons.BAOSTOCK_SERVER_PORT))
    except Exception:
        # print("服务器连接失败，请稍后再试。")
        return None
    return sock


def send_msg(msg):
    """发送消息，并接受消息 """
    try:
        # default_socket = get_default_socket()
        if hasattr(context, "default_socket"):
            default_socket = getattr(context, "default_socket")
            if default_socket is not None:
                # str 类型 -> bytes 类型
                # msg = msg + "<![CDATA[]]>"  # 在消息结尾追加“消息之间的分隔符”，压缩时的分隔符
                msg = msg + "\n"  # 在消息结尾追加“消息之间的分隔符”，不压缩时的分隔符
                default_socket.send(bytes(msg, encoding='utf-8'))
                receive = b""
                while True:
                    recv = default_socket.recv(Default_Socket_Size)
                    receive += recv
                    # 判断是否读取完
                    if receive[-13:] == b"<![CDATA[]]>\n":  # 压缩时的结尾分隔符长度
                    # if receive[-1:] == b"\n":  # 不压缩时的结尾分隔符长度
                        break
                # return bytes.decode(zlib.decompress(receive))  # 进行解压
                head_bytes = receive[0:cons.MESSAGE_HEADER_LENGTH]
                head_str = bytes.decode(head_bytes)
                head_arr = head_str.split(cons.MESSAGE_SPLIT)
                if head_arr[1] in cons.COMPRESSED_MESSAGE_TYPE_TUPLE:
                    # 消息体需要解压
                    head_inner_length = int(head_arr[2])
                    body_str = bytes.decode(zlib.decompress(receive[cons.MESSAGE_HEADER_LENGTH:cons.MESSAGE_HEADER_LENGTH + head_inner_length]))
                    return head_str + body_str
                else:
                    return bytes.decode(receive)  # 不进行解压
            else:
                return None
        else:
            print("you don't login.")
    except Exception as ex:
        # print(ex)
        raise(ex)
        print("接收数据异常，请稍后再试。")


class SocketRealTimeUtil(object):
    """Socket实时工具类"""
    # 记录第一个被创建对象的引用
    instance = None
    # 记录是否执行过初始化动作
    init_flag = False

    def __new__(cls, *args, **kwargs):
        # 1. 判断类属性是否是空对象
        if cls.instance is None:
            # 2. 调用父类的方法，为第一个对象分配空间
            cls.instance = super().__new__(cls)
        # 3. 返回类属性保存的对象引用
        return cls.instance

    def __init__(self):
        SocketRealTimeUtil.init_flag = True

    def connect(self):
        """创建实时行情连接"""
        try:
            mySockect = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            mySockect.connect((cons.BAOSTOCK_SERVER_REAL_TIME_IP, cons.BAOSTOCK_SERVER_REAL_TIME_PORT))
        except Exception:
            # print("服务器连接失败，请稍后再试。")
            pass
        setattr(context, "socket_real_time", mySockect)


def get_real_time_socket():
    """获取实时行情连接"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((cons.BAOSTOCK_SERVER_REAL_TIME_IP, cons.BAOSTOCK_SERVER_REAL_TIME_PORT))
    except Exception:
        # print("服务器连接失败，请稍后再试。")
        return None
    return sock


def send_real_time_msg(msg):
    """发送实时行情消息，并接受消息 """
    try:
        # default_socket = get_default_socket()
        if hasattr(context, "socket_real_time"):
            default_socket = getattr(context, "socket_real_time")
            if default_socket is not None:
                # str 类型 -> bytes 类型
                msg = msg + "<![CDATA[]]>"  # 在消息结尾追加“消息之间的分隔符”
                default_socket.send(bytes(msg, encoding='utf-8'))
                if msg.split(cons.MESSAGE_SPLIT)[1] == cons.MESSAGE_TYPE_LOGOUT_REAL_TIME_REQUEST:
                    default_socket.close()
                    setattr(context, "default_socket", None)
                else:
                    # default_socket.close()
                    receive = b""
                    while True:
                        recv = default_socket.recv(Default_Socket_Size)
                        receive += recv
                        # 判断是否读取完
                        if receive[-12:] == b"<![CDATA[]]>":
                            break
                    # return bytes.decode(receive)
                    return bytes.decode(zlib.decompress(receive))
            else:
                return None
        else:
            print("you don't login.")
    except Exception as ex:
        # print(ex)
        raise(ex)
        print("接收数据异常，请稍后再试。")


def send_cancel_real_time_msg(msg):
    """发送取消订阅实时行情消息 """
    try:
        # default_socket = get_default_socket()
        if hasattr(context, "socket_real_time"):
            default_socket = getattr(context, "socket_real_time")

            if default_socket is not None:
                # str 类型 -> bytes 类型
                msg = msg + "<![CDATA[]]>"  # 在消息结尾追加“消息之间的分隔符”
                default_socket.send(bytes(msg, encoding='utf-8'))
            else:
                return None
        else:
            print("you don't login.")
    except Exception as ex:
        # print(ex)
        raise(ex)
        print("接收数据异常，请稍后再试。")


def real_time_subscibe_thread(receive, socket_real_time, data):
    # time.sleep(2)
    # receive = b""
    # data.fncallback(data)
    data.data = {}
    while True:
        try:
            recv = socket_real_time.recv(Default_Socket_Size)
        except ConnectionAbortedError:
            # 如果连接被断开，跳出循环，不再接收数据
            break
        receive += recv
        print(receive)
        # 判断是否读取完b"<![CDATA[]]>"
        if receive.find(b"<![CDATA[]]>"):

            if receive[-12:] == b"<![CDATA[]]>":
                # 如果接收的数据以'<![CDATA[]]>'结尾，则是一个完整的记录，
                recv_arr = receive.split(b"<![CDATA[]]>")
                receive = b""
            else:
                recv_arr = receive.split(b"<![CDATA[]]>")
                recv_arr = receive.split(b"<![CDATA[]]>")[0:(len(receive.split(b"<![CDATA[]]>")) - 1)]
                receive = receive.split(b"<![CDATA[]]>")[-1:][0]

            for real_time_byte in recv_arr:
                # 由于real_time_byte可能为b''，即为空，需要判断
                if len(real_time_byte) > 0:
                    try:
                        real_time = bytes.decode(zlib.decompress(real_time_byte))
                    except Exception as ex:
                        print(ex)
                        print("数据解压缩异常，请稍后再试或联系管理员。")
                    if real_time is None or real_time.strip() == "":
                        continue

                    print(real_time)

                    if real_time.find("<![CDATA[]]>"):
                        real_time_arr = real_time.split("<![CDATA[]]>")
                        for realtime_arr_item in real_time_arr:
                            msg_header = realtime_arr_item[0:cons.MESSAGE_HEADER_LENGTH]
                            msg_header_arr = msg_header.split(cons.MESSAGE_SPLIT)
                            msg_body_length = int(msg_header_arr[2])
                            msg_body = realtime_arr_item[cons.MESSAGE_HEADER_LENGTH:cons.MESSAGE_HEADER_LENGTH + msg_body_length]

                            receive_crc = realtime_arr_item[cons.MESSAGE_HEADER_LENGTH + msg_body_length:]  # 传递进来的CRC校验码
                            crc32str = zlib.crc32(bytes(msg_header+msg_body, "utf8"))  # 客户端计算的CRC校验码
                            if receive_crc == str(crc32str):

                                body_arr = msg_body.split(cons.MESSAGE_SPLIT)
                                try:
                                    data.error_code = body_arr[0]
                                    data.error_msg = body_arr[1]

                                    if cons.BSERR_SUCCESS == data.error_code:
                                        realtime_arr_item = body_arr[3]
                                        data.data[realtime_arr_item.split(",")[2]] = realtime_arr_item.split(',')
                                        data.error_code = data.error_code  # 错误代码
                                        data.error_msg = data.error_msg
                                        data.fncallback(data)
                                    # data.error_code = cons.BSERR_NO_LOGIN  # 错误代码
                                    # data.error_msg = ""
                                    data.data = {}
                                except IndexError:
                                    pass

                    else:
                        msg_header = real_time[0:cons.MESSAGE_HEADER_LENGTH]
                        msg_header_arr = msg_header.split(cons.MESSAGE_SPLIT)
                        msg_body_length = int(msg_header_arr[2])
                        msg_body = real_time[cons.MESSAGE_HEADER_LENGTH:cons.MESSAGE_HEADER_LENGTH + msg_body_length]

                        receive_crc = real_time[cons.MESSAGE_HEADER_LENGTH + msg_body_length:]  # 传递进来的CRC校验码
                        crc32str = zlib.crc32(bytes(msg_header+msg_body, "utf8"))  # 客户端计算的CRC校验码
                        if receive_crc == str(crc32str):

                            body_arr = msg_body.split(cons.MESSAGE_SPLIT)
                            try:
                                data.error_code = body_arr[0]
                                data.error_msg = body_arr[1]

                                if cons.BSERR_SUCCESS == data.error_code:
                                    real_time = body_arr[3]
                                    data.data[real_time.split(",")[2]] = real_time.split(',')
                                    data.error_code = data.error_code  # 错误代码
                                    data.error_msg = data.error_msg
                                    data.fncallback(data)
                                # data.error_code = cons.BSERR_NO_LOGIN  # 错误代码
                                # data.error_msg = ""
                                data.data = {}
                            except IndexError:
                                pass
                else:
                    pass
#                     print(real_time_byte)
#                     print(bytes.decode(real_time_byte))


def send_real_time_subscibe(msg, data):
    """发送实时行情消息，并接受消息 """
    try:
        # default_socket = get_default_socket()
        if hasattr(context, "socket_real_time"):
            socket_real_time = getattr(context, "socket_real_time")
            if socket_real_time is not None:
                # str 类型 -> bytes 类型
                msg = msg + "<![CDATA[]]>"  # 在消息结尾追加“消息之间的分隔符”
                socket_real_time.send(bytes(msg, encoding='utf-8'))
                receive = b""
                while True:
                    recv = socket_real_time.recv(Default_Socket_Size)
                    receive += recv
                    # 判断是否读取完
                    if receive[-12:] == b"<![CDATA[]]>":
                        # receive_data = bytes.decode(receive)
                        """ 解压  """
                        try:
                            receive_data = bytes.decode(zlib.decompress(receive[:-12]))
                        except Exception as ex:
                            receive = b""
                            print(ex)
                            print("数据解压缩异常，请稍后再试或联系管理员。")
                            continue

                        if receive_data is None or receive_data.strip() == "":
                            data.error_code = cons.BSERR_RECVSOCK_FAIL
                            data.error_msg = "网络接收错误。"
                            return data

                        msg_header = receive_data[0:cons.MESSAGE_HEADER_LENGTH]
                        msg_header_arr = msg_header.split(cons.MESSAGE_SPLIT)
                        msg_body_length = msg_header_arr[2]
                        msg_body = receive_data[cons.MESSAGE_HEADER_LENGTH:cons.MESSAGE_HEADER_LENGTH + int(msg_body_length)]

                        # header_arr = msg_header.split(cons.MESSAGE_SPLIT)
                        body_arr = msg_body.split(cons.MESSAGE_SPLIT)

                        data.error_code = body_arr[0]
                        data.error_msg = body_arr[1]

                        if cons.BSERR_SUCCESS == data.error_code:
                            # real_time = body_arr[3]
                            # data.data[real_time.split(",")[0]] = real_time.strip(',').split(',')
                            t = threading.Thread(target=real_time_subscibe_thread, args=(receive, socket_real_time, data))
                            t.start()
                            data.serial_id = t.ident
                        data.error_code = data.error_code  # 错误代码
                        data.error_msg = data.error_msg
                        break
                return data
            else:
                return None
        else:
            print("you don't login.")
    except Exception as ex:
        # print(ex)
        raise(ex)
        print("接收数据异常，请稍后再试。")
