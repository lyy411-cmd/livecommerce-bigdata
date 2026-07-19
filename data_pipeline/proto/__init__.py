# -*- coding: utf-8 -*-
"""
抖音直播 protobuf 解码模块

便捷导入:
    from data_pipeline.proto import (
        decode_websocket_frame,
        build_ack_frame,
        MESSAGE_PARSERS,
    )
"""

from .douyin_decoder import (
    # 主入口
    decode_websocket_frame,
    build_ack_frame,

    # 帧 / 消息解析
    parse_push_frame,
    decompress_payload,
    parse_response,
    parse_message,

    # 业务消息解析
    parse_user,
    parse_chat_message,
    parse_gift_message,
    parse_like_message,
    parse_member_message,
    parse_social_message,
    parse_room_stats,

    # 映射表
    MESSAGE_PARSERS,

    # 底层工具（高级用法）
    _read_varint,
    _encode_varint,
    _decode_fields,
    _get_field,
    _get_string,
    _get_int,
    _get_bytes,
)

__all__ = [
    'decode_websocket_frame',
    'build_ack_frame',
    'parse_push_frame',
    'decompress_payload',
    'parse_response',
    'parse_message',
    'parse_user',
    'parse_chat_message',
    'parse_gift_message',
    'parse_like_message',
    'parse_member_message',
    'parse_social_message',
    'parse_room_stats',
    'MESSAGE_PARSERS',
]
