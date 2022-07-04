import random
from collections import defaultdict
from dataclasses import dataclass, asdict
from typing import Optional, List, Any

import numpy
import rocksdb
from rocksdb.interfaces import Comparator, AssociativeMergeOperator
from tqdm import tqdm

from config import config as cf
from mtab_server.resources.db.utils import (
    ToBytesType,
    deserialize_key,
    serialize_key,
    deserialize_value,
    serialize_value,
    preprocess_data_before_dump,
    OperationRocksdb,
)
from mtab_server.utils import io_worker as iw


class OperatorUpdateSetBitMap(AssociativeMergeOperator):
    def merge(self, key: bytes, existing_value: bytes, value: bytes):
        if existing_value:
            existing_value = deserialize_value(
                existing_value, bytes_value=ToBytesType.INT_BITMAP
            )
            value = deserialize_value(value, bytes_value=ToBytesType.INT_BITMAP)

            return_obj = existing_value | value
            return_obj = serialize_value(return_obj, bytes_value=ToBytesType.INT_BITMAP)
            return_obj = (True, return_obj)
        else:
            return_obj = (True, value)
        return return_obj

    def name(self):
        return b"updateBITMAP"


class OperatorUpdateCounter(AssociativeMergeOperator):
    def merge(self, key: bytes, existing_value: bytes, value: bytes):
        if existing_value:
            existing_value = deserialize_value(existing_value)
            value = deserialize_value(value)

            return_obj = existing_value + value
            return_obj = serialize_value(return_obj)
            return_obj = (True, return_obj)
        else:
            return_obj = (True, value)
        return return_obj

    def name(self):
        return b"updateCounter"


# class StaticPrefix(rocksdb.interfaces.SliceTransform):
#     def __init__(self, prefix_len: int = 5):
#         self.prefix_len = prefix_len
#
#     def name(self):
#         return b"static"
#
#     def transform(self, src):
#         return_obj = (0, self.prefix_len)
#         return return_obj
#
#     def in_domain(self, src):
#         return len(src) >= self.prefix_len
#
#     def in_range(self, dst):
#         return len(dst) == self.prefix_len


class IntegerComparator(Comparator):
    def compare(self, a, b):
        a = deserialize_key(a, integerkey=True)
        b = deserialize_key(b, integerkey=True)
        if a < b:
            return -1
        if a > b:
            return 1
        if a == b:
            return 0

    def name(self):
        return b"IntegerComparator"


@dataclass
class DBSpec:
    name: str
    integerkey: bool = False
    is_64bit: bool = False
    bytes_value: bool = ToBytesType.OBJ
    compress_value: bool = False
    prefix_len: int = 0


class RocksDBWorker:
    def __init__(
        self,
        dir_db: str,
        db_schema: Optional[List[DBSpec]] = None,
        read_only: bool = False,
        buff_limit: int = cf.BUFF_LIMIT,
        create_new: bool = False,
    ):
        if dir_db[-1] != "/":
            dir_db = f"{dir_db}/"

        self.dir_db = dir_db
        iw.create_dir(self.dir_db)

        self.metadata_file = f"{self.dir_db}metadata.json"
        iw.create_dir(self.metadata_file)

        if create_new:
            if db_schema is None:
                raise ValueError

            self.db_schema = {db_spec.name: db_spec for db_spec in db_schema}
            self.buff_limit = buff_limit
            self.save_metadata_info(db_schema, buff_limit)
        else:
            self.db_schema, self.buff_limit = self.load_metadata_info()

        self.db_schema["db_size"] = DBSpec(name="db_size")

        opts = rocksdb.Options(
            create_if_missing=True,
            write_buffer_size=self.buff_limit,
            compression=rocksdb.CompressionType.lz4_compression,
        )
        if create_new:
            self.env = rocksdb.DB(dir_db, opts, read_only=read_only)
        else:

            column_families = self.get_column_family_opts()
            self.env = rocksdb.DB(
                dir_db, opts, read_only=read_only, column_families=column_families,
            )

        self.max_db = len(self.db_schema)
        self.dbs = self.init_column_families()
        self.init_db_sizes()
        self.buff = defaultdict(list)
        self.buff_size = 0

    def get_db(self, column_name: str):
        if self.dbs.get(column_name) is None:
            raise ValueError
        return self.dbs[column_name]

    def get_db_schema(self, column_name: str):
        if self.db_schema.get(column_name) is None:
            raise ValueError
        return self.db_schema[column_name]

    def save_metadata_info(self, db_schema: Optional[List[DBSpec]], buff_limit: int):
        json_obj = {
            "db_schema": [asdict(db_i) for db_i in db_schema],
            "buff_limit": buff_limit,
        }
        iw.save_json_file(self.metadata_file, json_obj)

    def load_metadata_info(self):
        json_obj = iw.read_json_file(self.metadata_file)
        db_schema = {obj["name"]: DBSpec(**obj) for obj in json_obj["db_schema"]}
        buff_limit = json_obj["buff_limit"]
        return db_schema, buff_limit

    def get_column_family_opts(self):
        db_dict = {}
        col_opts = rocksdb.ColumnFamilyOptions()
        col_opts.merge_operator = OperatorUpdateCounter()
        db_dict[b"db_size"] = col_opts

        for db_spec in self.db_schema.values():
            if db_spec.name == "db_size":
                continue

            if db_spec.integerkey:
                col_opts = rocksdb.ColumnFamilyOptions(comparator=IntegerComparator())
            else:
                col_opts = rocksdb.ColumnFamilyOptions()

            # if db_spec.prefix_len:
            #     col_opts.prefix_extractor = StaticPrefix(prefix_len=db_spec.prefix_len)

            if db_spec.bytes_value == ToBytesType.INT_BITMAP:
                col_opts.merge_operator = OperatorUpdateSetBitMap()

            db_dict[db_spec.name.encode(cf.ENCODING)] = col_opts
        return db_dict

    def init_column_families(self):
        db_dict = {}
        column = self.env.get_column_family(b"db_size")
        if column:
            db_dict["db_size"] = column
        else:
            col_opts = rocksdb.ColumnFamilyOptions()
            col_opts.merge_operator = OperatorUpdateCounter()
            db_dict["db_size"] = self.env.create_column_family(b"db_size", col_opts)

        for db_spec in self.db_schema.values():
            if db_spec.name == "db_size":
                continue
            column = self.env.get_column_family(db_spec.name.encode(cf.ENCODING))
            if column:
                db_dict[db_spec.name] = column
                continue
            if db_spec.integerkey:
                col_opts = rocksdb.ColumnFamilyOptions(comparator=IntegerComparator())
            else:
                col_opts = rocksdb.ColumnFamilyOptions()

            # if db_spec.prefix_len:
            #     col_opts.prefix_extractor = StaticPrefix(prefix_len=db_spec.prefix_len)

            if db_spec.bytes_value == ToBytesType.INT_BITMAP:
                col_opts.merge_operator = OperatorUpdateSetBitMap()

            db_dict[db_spec.name] = self.env.create_column_family(
                db_spec.name.encode(cf.ENCODING), col_opts
            )
        return db_dict

    def init_db_sizes(self):
        for db_spec in self.db_schema.values():
            if db_spec.name != "db_size":
                cur_value = self.get_value("db_size", db_spec.name)
                if cur_value is None:
                    self.put("db_size", key=db_spec.name, value=0)

    def get_column_size(self, column_name):
        # db_size = int(
        #     self.env.get_property(b"rocksdb.estimate-num-keys").decode(cf.ENCODING)
        # )
        db_schema = self.get_db_schema(column_name)
        db_size = self.get_value("db_size", key_obj=db_schema.name)
        return db_size

    def view(self):
        stats = {
            column_name: self.get_column_size(column_name)
            for column_name in self.dbs.keys()
            if column_name != "db_size"
        }
        return stats

    def compact(self):
        for column_name in self.dbs.keys():
            if column_name == "db_size":
                continue

            self.env.compact_range(column_family=self.get_db(column_name))

    def close(self):
        self.env.close()

    def get_random_item(self, column_name):
        key = self.get_random_key(column_name)
        value = self.get_value(column_name, key)
        return key, value

    def get_random_value(self, column_name):
        key = self.get_random_key(column_name)
        value = self.get_value(column_name, key)
        return value

    def get_random_key(self, column_name):
        db = self.get_db(column_name)
        db_schema = self.get_db_schema(column_name)
        db_size = self.get_column_size(column_name)
        random_index = random.randint(0, db_size)

        cur = self.env.iterkeys(db)
        cur.seek_to_first()
        n_steps = random_index
        i = 0
        db_item = None
        key = None
        for item in cur:
            if i >= n_steps:
                db_item = item
                break
            i += 1

        if isinstance(db_item, tuple):
            key = db_item[1]

        if key is None:
            return None
        key = deserialize_key(
            key, integerkey=db_schema.integerkey, is_64bit=db_schema.is_64bit
        )
        return key

    def get_values(self, column_name, key_objs, get_deserialize=True, to_list=False):
        db = self.get_db(column_name)
        db_schema = self.get_db_schema(column_name)
        if isinstance(key_objs, numpy.ndarray):
            key_objs = key_objs.tolist()
        responds = dict()

        if not (
            isinstance(key_objs, list)
            or isinstance(key_objs, set)
            or isinstance(key_objs, tuple)
        ):
            return responds

        key_objs_bytes = [
            (db, serialize_key(k, integerkey=db_schema.integerkey)) for k in key_objs
        ]
        values = self.env.multi_get(key_objs_bytes)
        for i, v in enumerate(values.values()):
            if not v:
                continue
            if get_deserialize:
                try:
                    v = deserialize_value(
                        v,
                        bytes_value=db_schema.bytes_value,
                        compress_value=db_schema.compress_value,
                    )
                    if to_list and db_schema.bytes_value in {
                        ToBytesType.INT_BITMAP,
                        ToBytesType.INT_NUMPY,
                    }:
                        v = list(v)
                except Exception as message:
                    print(message)
            responds[key_objs[i]] = v

        return responds

    def get_value(self, column_name, key_obj, get_deserialize=True, to_list=False):
        db_schema = self.get_db_schema(column_name)
        db = self.get_db(column_name)
        key_obj = serialize_key(
            key_obj, integerkey=db_schema.integerkey, is_64bit=db_schema.is_64bit
        )

        if not key_obj:
            return None
        responds = None

        try:
            value_obj = self.env.get((db, key_obj))
            if not value_obj:
                return None
            responds = value_obj
            if get_deserialize:
                responds = deserialize_value(
                    value_obj,
                    bytes_value=db_schema.bytes_value,
                    compress_value=db_schema.compress_value,
                )
                if to_list and db_schema.bytes_value in {
                    ToBytesType.INT_BITMAP,
                    ToBytesType.INT_NUMPY,
                }:
                    responds = list(responds)
        except Exception as message:
            print(message)
        return responds

    def head(
        self, column_name, n, from_i=0,
    ):
        respond = defaultdict()
        for i, (k, v) in enumerate(self.iter_db(column_name, from_i=from_i)):
            respond[k] = v
            if i == n - 1:
                break
        return respond

    def iter_db_prefix(self, column_name, prefix, get_values=True, to_list=False):
        db = self.get_db(column_name)
        db_schema = self.get_db_schema(column_name)

        if get_values:
            cur = self.env.iteritems(db)
        else:
            cur = self.env.iterkeys(db)

        prefix = serialize_key(
            prefix, integerkey=db_schema.integerkey, is_64bit=db_schema.is_64bit
        )
        cur.seek(prefix)
        for db_obj in cur:
            key, value = None, None
            if get_values:
                key, value = db_obj
            else:
                key = db_obj
            key = key[1]
            if not key.startswith(prefix):
                break
            try:
                key = deserialize_key(
                    key, integerkey=db_schema.integerkey, is_64bit=db_schema.is_64bit,
                )
                if get_values:
                    value = deserialize_value(
                        value,
                        bytes_value=db_schema.bytes_value,
                        compress_value=db_schema.compress_value,
                    )
                    if to_list and db_schema.bytes_value in {
                        ToBytesType.INT_BITMAP,
                        ToBytesType.INT_NUMPY,
                    }:
                        value = list(value)
                    yield key, value
                else:
                    yield key
            except Exception as message:
                print(message)

    def iter_db(
        self,
        column_name,
        get_keys=True,
        get_values=True,
        deserialize_obj=True,
        from_i=0,
        to_i=-1,
        to_list=False,
    ):
        db = self.get_db(column_name)
        db_schema = self.get_db_schema(column_name)

        if to_i == -1:
            to_i = self.get_column_size(column_name)

        if get_values and get_keys:
            cur = self.env.iteritems(db)
        elif get_values:
            cur = self.env.itervalues(db)
        else:
            cur = self.env.iterkeys(db)

        if db_schema.integerkey and from_i != 0:
            cur.seek(serialize_key(from_i, integerkey=True))
            i = from_i
        else:
            i = 0
            cur.seek_to_first()

        for db_obj in cur:
            if i < from_i:
                i += 1
                continue
            if i >= to_i:
                break
            key, value = None, None

            try:
                if get_values and get_keys:
                    key, value = db_obj
                    key = key[1]
                elif get_values:
                    value = db_obj
                else:
                    key = db_obj
                    key = key[1]
                if key is not None:
                    key = deserialize_key(
                        key,
                        integerkey=db_schema.integerkey,
                        is_64bit=db_schema.is_64bit,
                    )
                if value is not None and deserialize_obj:
                    value = deserialize_value(
                        value,
                        bytes_value=db_schema.bytes_value,
                        compress_value=db_schema.compress_value,
                    )
                    if to_list and db_schema.bytes_value in {
                        ToBytesType.INT_BITMAP,
                        ToBytesType.INT_NUMPY,
                    }:
                        value = list(value)

                if get_values and get_keys:
                    return_obj = (key, value)
                    yield return_obj
                elif get_values:
                    yield value
                else:
                    yield key
            except UnicodeDecodeError:
                print(f"UnicodeDecodeError: {i}")
            except Exception:
                print(i)
                raise Exception
            i += 1

    def is_available(self, column_name, key_obj):
        db = self.get_db(column_name)
        db_schema = self.get_db_schema(column_name)
        if not isinstance(key_obj, bytes):
            key_obj = serialize_key(
                key_obj, integerkey=db_schema.integerkey, is_64bit=db_schema.is_64bit
            )
        if key_obj:
            try:
                value_obj = self.env.get((db, key_obj))
                if value_obj:
                    return True
            except Exception as message:
                print(message)
        return False

    def call_back_rocksdb_func(self, func, column_name, key, value=None):
        db = self.get_db(column_name)
        db_schema = self.get_db_schema(column_name)
        key = serialize_key(
            key, integerkey=db_schema.integerkey, is_64bit=db_schema.is_64bit
        )
        if value is not None:
            value = serialize_value(
                value,
                bytes_value=db_schema.bytes_value,
                compress_value=db_schema.compress_value,
            )
            func((db, key), value)
        else:
            func((db, key))

    def call_back_rocksdb_func_batch(
        self,
        operation: OperationRocksdb,
        items: Any,
        message: str = "",
        buff_limit=cf.SIZE_1GB,
        show_progress=True,
        step=1000,
    ):
        batch = rocksdb.WriteBatch()
        buff_size = 0
        p_bar = None

        def update_desc():
            return f"{message} buffer: {buff_size / buff_limit * 100:.0f}%"

        if show_progress:
            p_bar = tqdm(desc=update_desc(), total=len(items))

        for i, item in enumerate(items):
            if show_progress:
                p_bar.update()
                if i and i % step == 0:
                    p_bar.set_description(desc=update_desc())

            if operation == OperationRocksdb.DELETE:
                k = item
                batch.delete(k)
            else:
                k, v = item
                buff_size += len(k) + len(v)
                if operation == OperationRocksdb.PUT:
                    batch.put(k, v)
                elif operation == OperationRocksdb.MERGE:
                    batch.merge(k, v)

            if buff_size >= buff_limit:
                self.env.write(batch)
                batch.clear()
                buff_size = 0

        if buff_size:
            self.env.write(batch)
            batch.clear()

        if show_progress:
            p_bar.set_description(desc=update_desc())
            p_bar.close()

    def put(self, column_name, key, value, update_db_size=True):
        self.call_back_rocksdb_func(self.env.put, column_name, key, value)
        if update_db_size and column_name != "db_size":
            self.merge("db_size", column_name, 1)

    def merge(self, column_name, key, value, update_db_size=True):
        is_available = self.is_available(column_name, key)
        self.call_back_rocksdb_func(self.env.merge, column_name, key, value)
        if update_db_size and column_name != "db_size" and not is_available:
            self.merge("db_size", column_name, 1)

    def delete(self, column_name, key, update_db_size=True):
        is_available = self.is_available(column_name, key)
        self.call_back_rocksdb_func(self.env.delete, column_name, key)
        if update_db_size and column_name != "db_size" and is_available:
            self.merge("db_size", column_name, -1)

    def put_batch(
        self,
        column_name,
        items,
        sort_key=True,
        check_exist=True,
        buff_limit=cf.SIZE_1GB,
        show_progress=True,
        step=1000,
        message: str = "Write",
    ):
        db = self.get_db(column_name)
        db_schema = self.get_db_schema(column_name)

        items = preprocess_data_before_dump(
            items,
            bytes_value=db_schema.bytes_value,
            integerkey=db_schema.integerkey,
            is_64bit=db_schema.is_64bit,
            compress_value=db_schema.compress_value,
            sort_key=sort_key,
        )
        if check_exist:
            count_available = sum(
                1 for key, _ in items if self.is_available(column_name, key)
            )
            update_db_size = len(items) - count_available
        else:
            update_db_size = len(items)
        # combine column with key
        items = [((db, k), v) for k, v in items]
        self.call_back_rocksdb_func_batch(
            operation=OperationRocksdb.PUT,
            items=items,
            message=message,
            buff_limit=buff_limit,
            show_progress=show_progress,
            step=step,
        )
        if update_db_size:
            self.merge("db_size", column_name, update_db_size)

    def delete_batch(
        self,
        column_name,
        keys,
        buff_limit=cf.SIZE_1GB,
        show_progress=True,
        step=1000,
        message: str = "Delete",
        check_exist=True,
    ):
        db = self.get_db(column_name)
        db_schema = self.get_db_schema(column_name)
        keys = [
            serialize_key(
                key, integerkey=db_schema.integerkey, is_64bit=db_schema.is_64bit
            )
            for key in keys
        ]

        if check_exist:
            count_available = sum(
                1 for key in keys if self.is_available(column_name, key)
            )
            update_db_size = -count_available
        else:
            update_db_size = -len(keys)

        # combine column with key
        keys = [(db, k) for k, v in keys]

        self.call_back_rocksdb_func_batch(
            operation=OperationRocksdb.DELETE,
            items=keys,
            message=message,
            buff_limit=buff_limit,
            show_progress=show_progress,
            step=step,
        )
        if update_db_size:
            self.merge("db_size", column_name, update_db_size)

    def merge_batch(
        self,
        column_name,
        items,
        sort_key=True,
        check_exist=True,
        buff_limit=cf.SIZE_1GB,
        show_progress=True,
        step=1000,
        message: str = "Merge",
    ):
        db = self.get_db(column_name)
        db_schema = self.get_db_schema(column_name)

        items = preprocess_data_before_dump(
            items,
            bytes_value=db_schema.bytes_value,
            integerkey=db_schema.integerkey,
            is_64bit=db_schema.is_64bit,
            compress_value=db_schema.compress_value,
            sort_key=sort_key,
        )
        if check_exist:
            count_available = sum(
                1 for key, _ in items if self.is_available(column_name, key)
            )
            update_db_size = len(items) - count_available
        else:
            update_db_size = len(items)
        # combine column with key
        items = [((db, k), v) for k, v in items]
        self.call_back_rocksdb_func_batch(
            operation=OperationRocksdb.MERGE,
            items=items,
            message=message,
            buff_limit=buff_limit,
            show_progress=show_progress,
            step=step,
        )

        if update_db_size:
            self.merge("db_size", column_name, update_db_size)

    def drop_column(self, column_name):
        db = self.get_db(column_name)
        self.env.drop_column_family(db)
        self.put("db_size", column_name, 0)


#
# dir_db = "/tmp/hugedict/test"
# if os.path.isdir(dir_db):
#     shutil.rmtree(dir_db, ignore_errors=True)
# iw.create_dir(dir_db)
#
# opts = rocksdb.Options(
#     create_if_missing=True,
#     write_buffer_size=cf.SIZE_1MB,
#     compression=rocksdb.CompressionType.lz4_compression,
# )
#
#
# db = rocksdb.DB(dir_db, opts)
# cf_a = db.create_column_family(b"A", rocksdb.ColumnFamilyOptions())
# cf_b = db.create_column_family(
#     b"B", rocksdb.ColumnFamilyOptions(comparator=IntegerComparator())
# )
#
# columns = db.column_families
# columns_names = [handle.name for handle in columns]
#
#
# cfa = db.get_column_family(b"A")
# batch = rocksdb.WriteBatch()
# batch.put((cfa, b"key2"), b"v1")
# batch.put((cfa, b"key1"), b"v2")
# batch.put((cfa, b"key3"), b"v3")
# batch.put((cfa, b"c"), b"1")
# batch.put((cfa, b"a"), b"2")
# batch.put((cfa, b"b"), b"2")
# db.write(batch)
# batch.clear()
#
# items = db.multi_get([(cfa, b"key"), (cfa, b"c")])
#
# it = db.iteritems(cfa)
# it.seek_to_first()
# items1 = list(it)
#
# cfb = db.get_column_family(b"B")
#
#
# def int_to_bytes(ob):
#     return str(ob).encode("ascii")
#
#
# for x in range(50):
#     batch.put((cfb, int_to_bytes(x)), int_to_bytes(x))
# db.write(batch)
# batch.clear()
#
# it = db.iteritems(cfb)
# it.seek(b"10")
# items2 = list(it)
#
#
# db.compact_range(column_family=cfb)
# debug = 1
