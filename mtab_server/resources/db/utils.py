import pickle
import struct
from typing import Any

import msgpack
import numpy
from lz4 import frame
from pyroaring import BitMap
from config import config as cf


class OperationRocksdb:
    PUT = 1
    DELETE = 2
    MERGE = 3


class ToBytesType:
    OBJ = 0
    INT_NUMPY = 1
    INT_BITMAP = 2
    BYTES = 3
    PICKLE = 4


class DBUpdateType:
    SET = 0
    COUNTER = 1


def is_byte_obj(obj: Any):
    if isinstance(obj, bytes) or isinstance(obj, bytearray):
        return True
    return False


def set_default(obj: Any):
    if isinstance(obj, set):
        return sorted(list(obj))
    raise TypeError


def deserialize_key(key: Any, integerkey: bool = False, is_64bit: bool = False):
    if not integerkey:
        if isinstance(key, memoryview):
            key = key.tobytes()
        return key.decode(cf.ENCODING)
    if is_64bit:
        return struct.unpack("Q", key)[0]
    else:
        return struct.unpack("I", key)[0]


def deserialize_value(
    value: Any, bytes_value: ToBytesType = ToBytesType.OBJ, compress_value: bool = False
):
    if bytes_value == ToBytesType.INT_NUMPY:
        value = numpy.frombuffer(value, dtype=numpy.uint32)

    elif bytes_value == ToBytesType.INT_BITMAP:
        if not isinstance(value, bytes):
            value = bytes(value)
        value = BitMap.deserialize(value)

    elif bytes_value == ToBytesType.BYTES:
        if isinstance(value, memoryview):
            value = value.tobytes()

    else:  # mode == "msgpack"
        if compress_value:
            try:
                value = frame.decompress(value)
            except RuntimeError:
                pass
        if bytes_value == ToBytesType.PICKLE:
            value = pickle.loads(value)
        else:
            value = msgpack.unpackb(value, strict_map_key=False)
    return value


def deserialize(
    key: Any,
    value: Any,
    integerkey: bool = False,
    is_64bit: bool = False,
    bytes_value: ToBytesType = ToBytesType.OBJ,
    compress_value: bool = False,
):
    key = deserialize_key(key, integerkey, is_64bit)
    value = deserialize_value(value, bytes_value, compress_value)
    res_obj = (key, value)
    return res_obj


def serialize_key(key: Any, integerkey: bool = False, is_64bit: bool = False):
    if not integerkey:
        if not isinstance(key, str):
            key = str(key)
        return key.encode(cf.ENCODING)
    else:
        if not isinstance(key, int):
            raise TypeError
    if is_64bit:
        return struct.pack("Q", key)
    else:
        return struct.pack("I", key)


def serialize_value(
    value: Any,
    bytes_value: ToBytesType = ToBytesType.OBJ,
    compress_value: bool = False,
    sort_values: bool = True,
):
    if bytes_value == ToBytesType.INT_NUMPY:
        if sort_values:
            value = sorted(list(value))
        if not isinstance(value, numpy.ndarray):
            value = numpy.array(value, dtype=numpy.uint32)
        value = value.tobytes()

    elif bytes_value == ToBytesType.INT_BITMAP:
        value = BitMap(value).serialize()

    else:  # mode == "msgpack"
        if bytes_value == ToBytesType.PICKLE:
            value = pickle.dumps(value)
        else:
            if not isinstance(value, bytes) and not isinstance(value, bytearray):
                value = msgpack.packb(value, default=set_default)
        if compress_value:
            value = frame.compress(value)

    return value


def serialize(
    key: Any,
    value: Any,
    integerkey: bool = False,
    is_64bit: bool = False,
    bytes_value: ToBytesType = ToBytesType.OBJ,
    compress_value: bool = False,
):
    key = serialize_key(key, integerkey, is_64bit)
    value = serialize_value(value, bytes_value, compress_value)
    res_obj = (key, value)
    return res_obj


def preprocess_data_before_dump(
    data: Any,
    integerkey: bool = False,
    is_64bit: bool = False,
    bytes_value: ToBytesType = ToBytesType.OBJ,
    compress_value: bool = False,
    sort_key: bool = True,
):
    if isinstance(data, dict):
        data = list(data.items())

    if sort_key and integerkey:
        data.sort(key=lambda x: x[0])

    first_key, first_value = data[0]
    to_bytes_key = not is_byte_obj(first_key)
    to_bytes_value = not is_byte_obj(first_value)

    tmp_data = []
    for k, v in data:
        if k is None:
            continue
        if to_bytes_key:
            k = serialize_key(k, integerkey=integerkey, is_64bit=is_64bit,)
        if to_bytes_value:
            v = serialize_value(
                v, bytes_value=bytes_value, compress_value=compress_value,
            )
        tmp_data.append((k, v))
    data = tmp_data

    if sort_key and not integerkey:
        data.sort(key=lambda x: x[0])

    return data
