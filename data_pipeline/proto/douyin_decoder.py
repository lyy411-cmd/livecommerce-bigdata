# -*- coding: utf-8 -*-
"""
抖音直播 WebSocket 消息解码器
直接解析 protobuf 二进制线格式（wire format），无需 protoc 编译 .proto 文件。

支持的帧结构:
    PushFrame  -> gzip 解压 -> Response -> Message[] -> 各类型业务消息
"""

import gzip
import struct
from typing import Any, Dict, List, Optional, Tuple


# ============================================================
#  Protobuf 线格式基础解码
# ============================================================

def _read_varint(data: bytes, pos: int) -> Tuple[int, int]:
    """
    读取 protobuf varint 编码的无符号整数。

    :param data: 原始字节数据
    :param pos:  当前读取偏移量
    :return: (解码后的整数值, 新的偏移量)
    :raises ValueError: 数据不完整时抛出
    """
    result = 0
    shift = 0
    while pos < len(data):
        b = data[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            return result, pos
        shift += 7
    raise ValueError("varint 数据不完整，提前到达末尾")


def _encode_varint(value: int) -> bytes:
    """将非负整数编码为 protobuf varint 字节序列。"""
    if value < 0:
        raise ValueError("varint 不能为负数")
    if value == 0:
        return b'\x00'
    parts: List[int] = []
    while value > 0x7F:
        parts.append((value & 0x7F) | 0x80)
        value >>= 7
    parts.append(value & 0x7F)
    return bytes(parts)


def _decode_fields(data: bytes) -> Dict[int, List[Tuple[int, Any]]]:
    """
    解析 protobuf 二进制数据中的所有字段。

    每个字段以 (tag) 开头，tag = (field_number << 3) | wire_type。
    支持的 wire_type:
        0 - Varint   -> int
        1 - 64-bit   -> int (little-endian uint64)
        2 - LEN      -> bytes (length-delimited: string / bytes / embedded msg)
        5 - 32-bit   -> int (little-endian uint32)

    :param data: protobuf 编码的字节流
    :return: {field_number: [(wire_type, value), ...]}
    """
    fields: Dict[int, List[Tuple[int, Any]]] = {}
    pos = 0
    length = len(data)

    while pos < length:
        try:
            tag, pos = _read_varint(data, pos)
        except ValueError:
            break

        field_number = tag >> 3
        wire_type = tag & 0x07

        if wire_type == 0:  # Varint
            value, pos = _read_varint(data, pos)
        elif wire_type == 1:  # 64-bit fixed
            if pos + 8 > length:
                break
            value = struct.unpack_from('<Q', data, pos)[0]
            pos += 8
        elif wire_type == 2:  # Length-delimited
            str_len, pos = _read_varint(data, pos)
            if pos + str_len > length:
                break
            value = data[pos:pos + str_len]
            pos += str_len
        elif wire_type == 3:  # Start group (deprecated, skip value)
            value = None
        elif wire_type == 4:  # End group
            continue
        elif wire_type == 5:  # 32-bit fixed
            if pos + 4 > length:
                break
            value = struct.unpack_from('<I', data, pos)[0]
            pos += 4
        else:
            # 未知 wire_type，安全退出
            break

        fields.setdefault(field_number, []).append((wire_type, value))

    return fields


# ============================================================
#  字段辅助读取器
# ============================================================

def _get_field(fields: dict, field_number: int, default=None):
    """获取指定字段的第一个值，不存在时返回 default。"""
    entries = fields.get(field_number)
    if entries:
        return entries[0][1]
    return default


def _get_string(fields: dict, field_number: int, default: str = '') -> str:
    """获取 wire_type=2 的字段并解码为 UTF-8 字符串。"""
    val = _get_field(fields, field_number)
    if val is None:
        return default
    if isinstance(val, bytes):
        return val.decode('utf-8', errors='replace')
    return str(val)


def _get_int(fields: dict, field_number: int, default: int = 0) -> int:
    """获取 varint (wire_type=0) 字段的整数值。"""
    val = _get_field(fields, field_number)
    if val is None:
        return default
    return int(val)


def _get_bytes(fields: dict, field_number: int, default: bytes = b'') -> bytes:
    """获取原始字节字段（嵌套消息体）。"""
    val = _get_field(fields, field_number)
    if val is None:
        return default
    if isinstance(val, bytes):
        return val
    return default


# ============================================================
#  PushFrame / Response / Message 解析
# ============================================================

def parse_push_frame(data: bytes) -> Dict[str, Any]:
    """
    解析 PushFrame（WebSocket 最外层帧）。

    protobuf 字段映射:
        field 2 -> logId           (varint)     请求日志 ID
        field 6 -> payloadEncoding (string)     压缩方式，通常为 "gzip"
        field 8 -> payload         (bytes)      压缩后的 Response 消息体
    """
    fields = _decode_fields(data)
    return {
        'log_id': _get_int(fields, 2),
        'payload_encoding': _get_string(fields, 6, 'gzip'),
        'payload': _get_bytes(fields, 8),
    }


def decompress_payload(payload: bytes, encoding: str = 'gzip') -> bytes:
    """
    对 payload 进行 gzip 解压缩。
    若解压失败（如数据本未压缩），则原样返回。
    
    注意：抖音 WebSocket 帧的 payloadEncoding 字段可能标记为 'pb'，
    但实际 payload 仍为 gzip 压缩数据，因此始终尝试 gzip 解压。
    """
    if payload:
        try:
            return gzip.decompress(payload)
        except Exception:
            return payload
    return payload


def parse_response(data: bytes) -> Dict[str, Any]:
    """
    解析 Response 消息（PushFrame payload 解压后的内容）。

    protobuf 字段映射:
        field 1 -> messagesList (repeated bytes)  嵌套 Message 列表
        field 2 -> cursor       (string)          分页游标
        field 5 -> internalExt  (string)          ACK 所需扩展信息
        field 9 -> needAck      (bool / varint)   是否需要回复 ACK
    """
    fields = _decode_fields(data)

    # messagesList 是 repeated 字段，收集所有 field_number=1 的条目
    raw_messages = fields.get(1, [])
    messages_list = [
        entry[1] for entry in raw_messages
        if isinstance(entry[1], bytes)
    ]

    return {
        'messages_list': messages_list,
        'cursor': _get_string(fields, 2),
        'need_ack': bool(_get_int(fields, 9, 0)),
        'internal_ext': _get_string(fields, 5),
    }


def parse_message(data: bytes) -> Dict[str, Any]:
    """
    解析单条 Message（messagesList 中的元素）。

    protobuf 字段映射:
        field 1 -> method  (string)  消息类型标识，如 "WebcastChatMessage"
        field 2 -> payload (bytes)   具体业务消息体
    """
    fields = _decode_fields(data)
    return {
        'method': _get_string(fields, 1),
        'payload': _get_bytes(fields, 2),
    }


# ============================================================
#  用户信息解析
# ============================================================

def parse_user(data: bytes) -> Dict[str, Any]:
    """
    解析 User 嵌套消息。

    protobuf 字段映射:
        field 1 -> id       (varint)  用户唯一 ID
        field 3 -> nickname (string)  用户昵称
        field 6 -> level    (varint)  用户等级
    """
    fields = _decode_fields(data)
    return {
        'id': _get_int(fields, 1),
        'nickname': _get_string(fields, 3),
        'level': _get_int(fields, 6),
    }


# ============================================================
#  各类直播消息解析
# ============================================================

def parse_chat_message(data: bytes) -> Dict[str, Any]:
    """
    解析弹幕 / 聊天消息 (WebcastChatMessage)。

    protobuf 字段映射:
        field 2 -> user    (bytes, User)  发送者
        field 3 -> content (string)       弹幕文本内容
    """
    fields = _decode_fields(data)
    user_bytes = _get_bytes(fields, 2)
    user = parse_user(user_bytes) if user_bytes else {}
    return {
        'type': 'comment',
        'user': user,
        'content': _get_string(fields, 3),
    }


def parse_gift_message(data: bytes) -> Dict[str, Any]:
    """
    解析礼物消息 (WebcastGiftMessage)。

    protobuf 字段映射:
        field  2 -> giftId      (varint)          礼物 ID
        field  5 -> repeatCount (varint)          连击次数
        field  7 -> user        (bytes, User)     送礼用户
        field 15 -> gift        (bytes, nested)   礼物详情
                     gift.field 2 -> name         (string) 礼物名称
                     gift.field 4 -> diamondCount (varint) 抖币价值
    """
    fields = _decode_fields(data)
    user_bytes = _get_bytes(fields, 7)
    user = parse_user(user_bytes) if user_bytes else {}

    gift_bytes = _get_bytes(fields, 15)
    gift_info: Dict[str, Any] = {}
    if gift_bytes:
        gift_fields = _decode_fields(gift_bytes)
        gift_info = {
            'name': _get_string(gift_fields, 2),
            'diamond_count': _get_int(gift_fields, 4),
        }

    return {
        'type': 'gift',
        'gift_id': _get_int(fields, 2),
        'repeat_count': _get_int(fields, 5, 1),
        'user': user,
        'gift': gift_info,
    }


def parse_like_message(data: bytes) -> Dict[str, Any]:
    """
    解析点赞消息 (WebcastLikeMessage)。

    protobuf 字段映射:
        field 2 -> count (varint)       点赞数量
        field 5 -> user  (bytes, User)  点赞用户
    """
    fields = _decode_fields(data)
    user_bytes = _get_bytes(fields, 5)
    user = parse_user(user_bytes) if user_bytes else {}
    return {
        'type': 'like',
        'count': _get_int(fields, 2, 1),
        'user': user,
    }


def parse_member_message(data: bytes) -> Dict[str, Any]:
    """
    解析进入直播间消息 (WebcastMemberMessage)。

    protobuf 字段映射:
        field 2 -> user        (bytes, User)  进入的用户
        field 3 -> memberCount (varint)       当前成员数 / 在线人数
    """
    fields = _decode_fields(data)
    user_bytes = _get_bytes(fields, 2)
    user = parse_user(user_bytes) if user_bytes else {}
    return {
        'type': 'enter',
        'user': user,
        'member_count': _get_int(fields, 3),
    }


def parse_social_message(data: bytes) -> Dict[str, Any]:
    """
    解析关注消息 (WebcastSocialMessage)。

    protobuf 字段映射:
        field 2 -> user (bytes, User)  关注主播的用户
    """
    fields = _decode_fields(data)
    user_bytes = _get_bytes(fields, 2)
    user = parse_user(user_bytes) if user_bytes else {}
    return {
        'type': 'follow',
        'user': user,
    }


def parse_room_stats(data: bytes) -> Dict[str, Any]:
    """
    解析直播间统计消息 (WebcastRoomStatsMessage)。

    protobuf 字段映射:
        field 2 -> displayLong  (string)  完整人数文本，如 "1.2万人"
        field 3 -> displayShort (string)  简短人数，如 "1.2万"
        field 4 -> displayValue (string)  纯数字字符串，如 "12000"
    """
    fields = _decode_fields(data)
    return {
        'type': 'stats',
        'display_long': _get_string(fields, 2),
        'display_short': _get_string(fields, 3),
        'display_value': _get_string(fields, 4),
    }


# ============================================================
#  消息方法名 -> 解析器映射表
# ============================================================

MESSAGE_PARSERS: Dict[str, Any] = {
    'WebcastChatMessage':      parse_chat_message,
    'WebcastGiftMessage':      parse_gift_message,
    'WebcastLikeMessage':      parse_like_message,
    'WebcastMemberMessage':    parse_member_message,
    'WebcastSocialMessage':    parse_social_message,
    'WebcastRoomStatsMessage': parse_room_stats,
}


# ============================================================
#  主解码入口
# ============================================================

def decode_websocket_frame(
    binary_data: bytes,
) -> Tuple[int, List[Dict[str, Any]], str, bool, str]:
    """
    解码完整的 WebSocket 二进制帧。

    处理流程:
        1. 解析 PushFrame（最外层）
        2. gzip 解压 payload
        3. 解析 Response 消息
        4. 逐条解析 messagesList 中的 Message

    :param binary_data: WebSocket 接收到的原始字节
    :return: (log_id, parsed_messages, cursor, need_ack, internal_ext)
             - log_id:        PushFrame 中的日志 ID
             - parsed_messages: 已解析的业务消息字典列表
             - cursor:        分页游标（用于下次请求）
             - need_ack:      是否需要发送 ACK
             - internal_ext:  ACK 帧所需的扩展字符串
    """
    # 1. 解析外层 PushFrame
    frame = parse_push_frame(binary_data)
    log_id = frame['log_id']
    encoding = frame.get('payload_encoding', 'gzip')
    payload = frame['payload']

    if not payload:
        return log_id, [], '', False, ''

    # 2. 解压 payload
    decompressed = decompress_payload(payload, encoding)

    # 3. 解析 Response
    response = parse_response(decompressed)
    cursor = response['cursor']
    need_ack = response['need_ack']
    internal_ext = response['internal_ext']

    # 4. 逐条解析 Message
    parsed_messages: List[Dict[str, Any]] = []
    for msg_bytes in response['messages_list']:
        try:
            msg = parse_message(msg_bytes)
            method = msg['method']
            msg_payload = msg['payload']

            parser = MESSAGE_PARSERS.get(method)
            if parser and msg_payload:
                parsed = parser(msg_payload)
                parsed['method'] = method
                parsed_messages.append(parsed)
            else:
                # 未知消息类型，保留方法名供上层处理
                parsed_messages.append({
                    'method': method,
                    'type': 'unknown',
                })
        except Exception:
            # 单条消息解析失败不影响其他消息的处理
            continue

    return log_id, parsed_messages, cursor, need_ack, internal_ext


# ============================================================
#  ACK 帧构建（心跳保活）
# ============================================================

def build_ack_frame(internal_ext: str) -> bytes:
    """
    构建 ACK PushFrame 用于心跳保活。

    ACK 帧是一个精简的 PushFrame，只包含:
        field 6 (payloadEncoding, string) = "ack"
        field 8 (payload,         bytes)  = internal_ext 的 UTF-8 编码

    直接手动编码 protobuf 线格式，无需序列化库。

    :param internal_ext: Response 中返回的 internalExt 字符串
    :return: 编码后的 PushFrame 字节
    """
    parts: List[bytes] = []

    # field 6, wire_type=2 (LEN) -> tag = (6 << 3) | 2 = 50
    encoding_bytes = b'ack'
    parts.append(bytes([50]))
    parts.append(_encode_varint(len(encoding_bytes)))
    parts.append(encoding_bytes)

    # field 8, wire_type=2 (LEN) -> tag = (8 << 3) | 2 = 66
    payload_bytes = internal_ext.encode('utf-8')
    parts.append(bytes([66]))
    parts.append(_encode_varint(len(payload_bytes)))
    parts.append(payload_bytes)

    return b''.join(parts)
