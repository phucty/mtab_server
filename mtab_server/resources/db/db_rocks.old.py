import copy
import pickle
import random
import struct
import zlib
from datetime import datetime
from multiprocessing import Pool

import msgpack
import numpy as np
from collections import defaultdict, Counter
from contextlib import closing
from time import sleep, time
import sys
import lmdb
from tqdm import tqdm

from lz4 import frame
import m_config as cf
from api.resources.m_item import MItem

from api.utilities import m_io as iw
import gc
from api.utilities import m_utils as ul
import os
import rocksdb

import psutil


def profile(func):
    def wrapper(*args, **kwargs):
        process = psutil.Process(os.getpid())
        mem_before = process.memory_info()
        start = datetime.now()

        result = func(*args, **kwargs)

        end = datetime.now() - start
        mem_after = process.memory_info()
        rss = iw.get_size_obj(mem_after.rss - mem_before.rss)
        vms = iw.get_size_obj(mem_after.vms - mem_before.vms)
        print(f"{func.__name__}\tRun time: {end}\tRSS: {rss}\tVMS: {vms}")
        return result

    return wrapper


def set_default(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError


def deserialize_key(key, integerkey=False):
    if not integerkey:
        return key.decode(cf.ENCODING)
    return struct.unpack("I", key)[0]


def deserialize_value(value, mode="msgpack", compress=True):
    if mode == "numpy":
        value = np.frombuffer(value, dtype=np.uint32)
    else:  # mode == "msgpack"
        if compress:
            value = frame.decompress(value)
        value = msgpack.unpackb(value)
    return value


def serialize_key(key, integerkey=False):
    if not integerkey:
        if not isinstance(key, str):
            key = str(key)
        return key.encode(cf.ENCODING)[: cf.LMDB_MAX_KEY]
    return struct.pack("I", key)


def serialize_value(value, mode="msgpack", compress=True):
    if mode == "numpy":
        value = np.array(list(value), dtype=np.uint32).tobytes()
    else:  # mode == "msgpack"
        value = msgpack.packb(value, default=set_default)
        if compress:
            value = frame.compress(value)

    return value


def serialize(key, value, integerkey=False, mode="msgpack", compress=True):
    return serialize_key(key, integerkey), serialize_value(value, mode, compress)


class DBSubspace:
    def __init__(self, db_dir, read_only=True):
        iw.create_dir(db_dir)
        self.db_name = db_dir
        self.db = rocksdb.DB(
            db_dir,
            rocksdb.Options(
                create_if_missing=True,
                write_buffer_size=cf.SIZE_512MB,
                compression=rocksdb.CompressionType.lz4_compression,
                # inplace_update_support=False,
                # max_open_files=300000,
                # max_write_buffer_number=3,
                # target_file_size_base=67108864,
                # table_factory=rocksdb.BlockBasedTableFactory(
                #     filter_policy=rocksdb.BloomFilterPolicy(10),
                #     block_cache=rocksdb.LRUCache(2 * (1024 ** 3)),
                #     block_cache_compressed=rocksdb.LRUCache(512 * (1024 ** 2)),
                # ),
            ),
            read_only=read_only,
        )

    def get_size(self):
        return int(
            self.db.get_property(b"rocksdb.estimate-num-keys").decode(cf.ENCODING)
        )

    def get_item(self, key, integerkey=False, mode="msgpack", compress=True):
        key = serialize_key(key, integerkey)
        item = self.db.get(key)
        if item is None:
            return None
        return deserialize_value(item, mode, compress)

    def get_items(self, keys, integerkey=False, mode="msgpack", compress=True):
        keys_s = [serialize_key(k, integerkey) for k in keys]
        responds = {
            k: deserialize_value(v, mode, compress)
            for k, v in self.db.multi_get(keys_s)
            if v is not None
        }
        return responds

    def set_item(self, key, value, integerkey=False, mode="msgpack", compress=True):
        key = serialize_key(key, integerkey)
        value = serialize_value(value, mode, compress)
        self.db.put(key, value)

    def set_items(
        self,
        items,
        integerkey=False,
        mode="msgpack",
        compress=True,
        buff_limit=cf.SIZE_1GB,
        show_progress=True,
        step=1000,
    ):
        if isinstance(items, dict):
            items = sorted(items.items(), key=lambda x: x[0])
        else:
            items.sort(key=lambda x: x[0])

        batch = rocksdb.WriteBatch()
        buff_size = 0
        p_bar = None

        def update_desc():
            return f"Saving: buff:{buff_size/buff_limit * 100:.2f}%"

        if show_progress:
            p_bar = tqdm(desc=update_desc())
        for i, (k, v) in enumerate(items):
            k = serialize_key(k, integerkey)
            v = serialize_value(v, mode, compress)
            buff_size += len(k) + len(v)
            batch.put(k, v)

            if buff_size >= buff_limit:
                self.db.write(batch)
                batch = rocksdb.WriteBatch()
                buff_size = 0

            if show_progress and i % step == 0:
                p_bar.update(step)
                p_bar.set_description(desc=update_desc())

        if buff_size:
            self.db.write(batch)
            p_bar.set_description(desc=update_desc())

        if show_progress:
            p_bar.close()

    def remove_value(self, key, integerkey=False):
        key = serialize_key(key, integerkey)
        self.db.delete(key)

    def keys(self, integerkey=False):
        iterator = self.db.iterkeys()
        iterator.seek_to_first()
        for k in iterator:
            yield deserialize_key(k, integerkey)

    def values(self, mode="msgpack", compress=True):
        iterator = self.db.itervalues()
        iterator.seek_to_first()
        for k in iterator:
            yield deserialize_value(k, mode, compress)

    def items(self, integerkey=False, mode="msgpack", compress=True):
        iterator = self.db.iteritems()
        iterator.seek_to_first()
        for k, v in iterator:
            yield deserialize_key(k, integerkey), deserialize_value(v, mode, compress)


class DBSpace:
    def __init__(self, db_dir, readonly=True):
        self.db_items = DBSubspace(db_dir + "/items", readonly)
        self.db_nums = DBSubspace(db_dir + "/nums", readonly)
        self.db_test = DBSubspace(db_dir + "/test", readonly)


def pool_multi_read(args):
    chunk_id, integerkey, mode, compress = args
    for i in range(n_open):
        for key in my_db[i].db_items.keys():
            value = my_db[i].db_items.get_item(
                key=key, integerkey=integerkey, mode=mode, compress=compress
            )

    # sleep(0.0001)
    return chunk_id


def iterator(n_chunks, integerkey=False, mode="msgpack", compress=True):
    for i in range(n_chunks):
        yield (i, integerkey, mode, compress)


@profile
def run_1_process(chunks=50):
    for args in iterator(chunks):
        chunk_id = pool_multi_read(args)
        # data_items[key] = value


@profile
def run_n_process(n_process=10, chunks=50):
    with closing(Pool(processes=n_process)) as p:
        for chunk_id in p.imap_unordered(pool_multi_read, iterator(chunks)):
            continue


def test_db():
    # limit = 3000
    # print(f"\nWiki Item data: {limit}")
    # test_db = DBSpace(db_dir=cf.DIR_TEST_ROCKSDB, readonly=False)
    # db_items = MItem()
    # data_items = []
    # for i, i_wd in enumerate(db_items.keys()):
    #     if i >= limit:
    #         break
    #     data_items.append((i_wd, db_items.get_item(i_wd)))
    # data_items.sort(key=lambda x: x[0])
    #
    # test_db.db_items.set_items(data_items)
    #
    # data_nums = {
    #     random.randint(0, 912162600): list(
    #         {random.randint(0, 912162600) for _ in range(random.randint(0, 500))}
    #     )
    #     for _ in range(1000)
    # }
    # data_nums = sorted(data_nums.items(), key=lambda x: x[0])
    # test_db.db_nums.set_items(data_nums, integerkey=True, mode="numpy")
    #
    global my_db
    global n_open

    n_open = 10
    my_db = {
        i: DBSpace(db_dir=cf.DIR_TEST_ROCKSDB, readonly=True) for i in range(n_open)
    }

    run_1_process()
    # run_n_process(n_process=1)
    run_n_process(n_process=12)

    # loaded_data_items = list(test_db.db_items.items())
    # loaded_data_nums = list(test_db.db_nums.items(integerkey=True, mode="numpy"))
    debug = 1


if __name__ == "__main__":
    test_db()


import os
import sys
import shutil
import gc
import unittest
import rocksdb
from itertools import takewhile
import struct
import tempfile
from rocksdb.merge_operators import UintAddOperator, StringAppendOperator


def int_to_bytes(ob):
    return str(ob).encode("ascii")


class TestHelper(unittest.TestCase):
    def setUp(self):
        self.db_loc = tempfile.mkdtemp()
        self.addCleanup(self._close_db)

    def _close_db(self):
        del self.db
        gc.collect()
        if os.path.exists(self.db_loc):
            shutil.rmtree(self.db_loc)


class TestDB(TestHelper):
    def setUp(self):
        TestHelper.setUp(self)
        opts = rocksdb.Options(create_if_missing=True)
        self.db = rocksdb.DB(os.path.join(self.db_loc, "test"), opts)

    def test_options_used_twice(self):
        if sys.version_info[0] == 3:
            assertRaisesRegex = self.assertRaisesRegex
        else:
            assertRaisesRegex = self.assertRaisesRegexp
        expected = "Options object is already used by another DB"
        with assertRaisesRegex(Exception, expected):
            rocksdb.DB(os.path.join(self.db_loc, "test2"), self.db.options)

    def test_unicode_path(self):
        name = os.path.join(self.db_loc, b"M\xc3\xbcnchen".decode("utf8"))
        rocksdb.DB(name, rocksdb.Options(create_if_missing=True))
        self.addCleanup(shutil.rmtree, name)
        self.assertTrue(os.path.isdir(name))

    def test_get_none(self):
        self.assertIsNone(self.db.get(b"xxx"))

    def test_put_get(self):
        self.db.put(b"a", b"b")
        self.assertEqual(b"b", self.db.get(b"a"))

    def test_put_then_get_from_secondary(self):
        secondary_location = os.path.join(self.db_loc, "secondary")
        secondary = rocksdb.DB(
            os.path.join(self.db_loc, "test"),
            rocksdb.Options(create_if_missing=True, max_open_files=-1),
            secondary_name=secondary_location,
        )
        self.addCleanup(secondary.close)

        self.assertIsNone(secondary.get(b"a"))
        self.db.put(b"a", b"b")
        self.assertEqual(b"b", self.db.get(b"a"))
        self.assertIsNone(secondary.get(b"a"))
        secondary.try_catch_up_with_primary()
        self.assertEqual(b"b", secondary.get(b"a"))

        secondary2_location = os.path.join(self.db_loc, "secondary2")
        secondary2 = rocksdb.DB(
            os.path.join(self.db_loc, "test"),
            rocksdb.Options(create_if_missing=True, max_open_files=-1),
            secondary_name=secondary2_location,
        )
        self.addCleanup(secondary2.close)

        self.assertEqual(b"b", secondary2.get(b"a"))
        self.db.put(b"a", b"c")
        self.assertEqual(b"b", secondary.get(b"a"))
        self.assertEqual(b"b", secondary2.get(b"a"))
        self.assertEqual(b"c", self.db.get(b"a"))
        secondary.try_catch_up_with_primary()
        secondary2.try_catch_up_with_primary()
        self.assertEqual(b"c", secondary.get(b"a"))
        self.assertEqual(b"c", secondary2.get(b"a"))

    def test_multi_get(self):
        self.db.put(b"a", b"1")
        self.db.put(b"b", b"2")
        self.db.put(b"c", b"3")

        ret = self.db.multi_get([b"a", b"b", b"c"])
        ref = {b"a": b"1", b"c": b"3", b"b": b"2"}
        self.assertEqual(ref, ret)

    def test_delete(self):
        self.db.put(b"a", b"b")
        self.assertEqual(b"b", self.db.get(b"a"))
        self.db.delete(b"a")
        self.assertIsNone(self.db.get(b"a"))

    def test_write_batch(self):
        batch = rocksdb.WriteBatch()
        batch.put(b"key", b"v1")
        batch.delete(b"key")
        batch.put(b"key", b"v2")
        batch.put(b"key", b"v3")
        batch.put(b"a", b"b")

        self.db.write(batch)
        ref = {b"a": b"b", b"key": b"v3"}
        ret = self.db.multi_get([b"key", b"a"])
        self.assertEqual(ref, ret)

    def test_write_batch_context(self):
        with self.db.write_batch() as batch:
            batch.put(b"key", b"v1")
            batch.delete(b"key")
            batch.put(b"key", b"v2")
            batch.put(b"key", b"v3")
            batch.put(b"a", b"b")

        ref = {b"a": b"b", b"key": b"v3"}
        ret = self.db.multi_get([b"key", b"a"])
        self.assertEqual(ref, ret)

    def test_write_batch_iter(self):
        batch = rocksdb.WriteBatch()
        self.assertEqual([], list(batch))

        batch.put(b"key1", b"v1")
        batch.put(b"key2", b"v2")
        batch.put(b"key3", b"v3")
        batch.delete(b"a")
        batch.delete(b"key1")
        batch.merge(b"xxx", b"value")

        it = iter(batch)
        del batch
        ref = [
            ("Put", b"key1", b"v1"),
            ("Put", b"key2", b"v2"),
            ("Put", b"key3", b"v3"),
            ("Delete", b"a", b""),
            ("Delete", b"key1", b""),
            ("Merge", b"xxx", b"value"),
        ]
        self.assertEqual(ref, list(it))

    def test_key_may_exists(self):
        self.db.put(b"a", b"1")

        self.assertEqual((False, None), self.db.key_may_exist(b"x"))
        self.assertEqual((False, None), self.db.key_may_exist(b"x", True))
        self.assertEqual((True, None), self.db.key_may_exist(b"a"))
        self.assertEqual((True, b"1"), self.db.key_may_exist(b"a", True))

    def test_seek_for_prev(self):
        self.db.put(b"a1", b"a1_value")
        self.db.put(b"a3", b"a3_value")
        self.db.put(b"b1", b"b1_value")
        self.db.put(b"b2", b"b2_value")
        self.db.put(b"c2", b"c2_value")
        self.db.put(b"c4", b"c4_value")

        self.assertEqual(self.db.get(b"a1"), b"a1_value")

        it = self.db.iterkeys()

        it.seek(b"a1")
        self.assertEqual(it.get(), b"a1")
        it.seek(b"a3")
        self.assertEqual(it.get(), b"a3")
        it.seek_for_prev(b"c4")
        self.assertEqual(it.get(), b"c4")
        it.seek_for_prev(b"c3")
        self.assertEqual(it.get(), b"c2")

        it = self.db.itervalues()
        it.seek(b"a1")
        self.assertEqual(it.get(), b"a1_value")
        it.seek(b"a3")
        self.assertEqual(it.get(), b"a3_value")
        it.seek_for_prev(b"c4")
        self.assertEqual(it.get(), b"c4_value")
        it.seek_for_prev(b"c3")
        self.assertEqual(it.get(), b"c2_value")

        it = self.db.iteritems()
        it.seek(b"a1")
        self.assertEqual(it.get(), (b"a1", b"a1_value"))
        it.seek(b"a3")
        self.assertEqual(it.get(), (b"a3", b"a3_value"))
        it.seek_for_prev(b"c4")
        self.assertEqual(it.get(), (b"c4", b"c4_value"))
        it.seek_for_prev(b"c3")
        self.assertEqual(it.get(), (b"c2", b"c2_value"))

        reverse_it = reversed(it)
        it.seek_for_prev(b"c3")
        self.assertEqual(it.get(), (b"c2", b"c2_value"))

    def test_iter_keys(self):
        for x in range(300):
            self.db.put(int_to_bytes(x), int_to_bytes(x))

        it = self.db.iterkeys()

        self.assertEqual([], list(it))

        it.seek_to_last()
        self.assertEqual([b"99"], list(it))

        ref = sorted([int_to_bytes(x) for x in range(300)])
        it.seek_to_first()
        self.assertEqual(ref, list(it))

        it.seek(b"90")
        ref = [b"90", b"91", b"92", b"93", b"94", b"95", b"96", b"97", b"98", b"99"]
        self.assertEqual(ref, list(it))

    def test_iter_values(self):
        for x in range(300):
            self.db.put(int_to_bytes(x), int_to_bytes(x * 1000))

        it = self.db.itervalues()

        self.assertEqual([], list(it))

        it.seek_to_last()
        self.assertEqual([b"99000"], list(it))

        ref = sorted([int_to_bytes(x) for x in range(300)])
        ref = [int_to_bytes(int(x) * 1000) for x in ref]
        it.seek_to_first()
        self.assertEqual(ref, list(it))

        it.seek(b"90")
        ref = [int_to_bytes(x * 1000) for x in range(90, 100)]
        self.assertEqual(ref, list(it))

    def test_iter_items(self):
        for x in range(300):
            self.db.put(int_to_bytes(x), int_to_bytes(x * 1000))

        it = self.db.iteritems()

        self.assertEqual([], list(it))

        it.seek_to_last()
        self.assertEqual([(b"99", b"99000")], list(it))

        ref = sorted([int_to_bytes(x) for x in range(300)])
        ref = [(x, int_to_bytes(int(x) * 1000)) for x in ref]
        it.seek_to_first()
        self.assertEqual(ref, list(it))

        it.seek(b"90")
        ref = [(int_to_bytes(x), int_to_bytes(x * 1000)) for x in range(90, 100)]
        self.assertEqual(ref, list(it))

    def test_reverse_iter(self):
        for x in range(100):
            self.db.put(int_to_bytes(x), int_to_bytes(x * 1000))

        it = self.db.iteritems()
        it.seek_to_last()

        ref = reversed(sorted([int_to_bytes(x) for x in range(100)]))
        ref = [(x, int_to_bytes(int(x) * 1000)) for x in ref]

        self.assertEqual(ref, list(reversed(it)))

    def test_snapshot(self):
        self.db.put(b"a", b"1")
        self.db.put(b"b", b"2")

        snapshot = self.db.snapshot()
        self.db.put(b"a", b"2")
        self.db.delete(b"b")

        it = self.db.iteritems()
        it.seek_to_first()
        self.assertEqual({b"a": b"2"}, dict(it))

        it = self.db.iteritems(snapshot=snapshot)
        it.seek_to_first()
        self.assertEqual({b"a": b"1", b"b": b"2"}, dict(it))

    def test_get_property(self):
        for x in range(300):
            x = int_to_bytes(x)
            self.db.put(x, x)

        self.assertIsNotNone(self.db.get_property(b"rocksdb.stats"))
        self.assertIsNotNone(self.db.get_property(b"rocksdb.sstables"))
        self.assertIsNotNone(self.db.get_property(b"rocksdb.num-files-at-level0"))
        self.assertIsNone(self.db.get_property(b"does not exsits"))

    def test_compact_range(self):
        for x in range(10000):
            x = int_to_bytes(x)
            self.db.put(x, x)

        self.db.compact_range()


class AssocCounter(rocksdb.interfaces.AssociativeMergeOperator):
    def merge(self, key, existing_value, value):
        if existing_value:
            return (True, int_to_bytes(int(existing_value) + int(value)))
        return (True, value)

    def name(self):
        return b"AssocCounter"


class TestUint64Merge(TestHelper):
    def setUp(self):
        TestHelper.setUp(self)
        opts = rocksdb.Options()
        opts.create_if_missing = True
        opts.merge_operator = UintAddOperator()
        self.db = rocksdb.DB(os.path.join(self.db_loc, "test"), opts)

    def test_merge(self):
        self.db.put(b"a", struct.pack("Q", 5566))
        for x in range(1000):
            self.db.merge(b"a", struct.pack("Q", x))
        self.assertEqual(
            5566 + sum(range(1000)), struct.unpack("Q", self.db.get(b"a"))[0]
        )


#  class TestPutMerge(TestHelper):
#  def setUp(self):
#  TestHelper.setUp(self)
#  opts = rocksdb.Options()
#  opts.create_if_missing = True
#  opts.merge_operator = "put"
#  self.db = rocksdb.DB(os.path.join(self.db_loc, 'test'), opts)

#  def test_merge(self):
#  self.db.put(b'a', b'ccc')
#  self.db.merge(b'a', b'ddd')
#  self.assertEqual(self.db.get(b'a'), 'ddd')

#  class TestPutV1Merge(TestHelper):
#  def setUp(self):
#  TestHelper.setUp(self)
#  opts = rocksdb.Options()
#  opts.create_if_missing = True
#  opts.merge_operator = "put_v1"
#  self.db = rocksdb.DB(os.path.join(self.db_loc, 'test'), opts)

#  def test_merge(self):
#  self.db.put(b'a', b'ccc')
#  self.db.merge(b'a', b'ddd')
#  self.assertEqual(self.db.get(b'a'), 'ddd')


class TestStringAppendOperatorMerge(TestHelper):
    def setUp(self):
        TestHelper.setUp(self)
        opts = rocksdb.Options()
        opts.create_if_missing = True
        opts.merge_operator = StringAppendOperator()
        self.db = rocksdb.DB(os.path.join(self.db_loc, "test"), opts)

    # NOTE(sileht): Raise "Corruption: Error: Could not perform merge." on PY3
    # @unittest.skipIf(sys.version_info[0] == 3,
    #                 "Unexpected behavior on PY3")
    def test_merge(self):
        self.db.put(b"a", b"ccc")
        self.db.merge(b"a", b"ddd")
        self.assertEqual(self.db.get(b"a"), b"ccc,ddd")


#  class TestStringMaxOperatorMerge(TestHelper):
#  def setUp(self):
#  TestHelper.setUp(self)
#  opts = rocksdb.Options()
#  opts.create_if_missing = True
#  opts.merge_operator = "max"
#  self.db = rocksdb.DB(os.path.join(self.db_loc, 'test'), opts)

#  def test_merge(self):
#  self.db.put(b'a', int_to_bytes(55))
#  self.db.merge(b'a', int_to_bytes(56))
#  self.assertEqual(int(self.db.get(b'a')), 56)


class TestAssocMerge(TestHelper):
    def setUp(self):
        TestHelper.setUp(self)
        opts = rocksdb.Options()
        opts.create_if_missing = True
        opts.merge_operator = AssocCounter()
        self.db = rocksdb.DB(os.path.join(self.db_loc, "test"), opts)

    def test_merge(self):
        for x in range(1000):
            self.db.merge(b"a", int_to_bytes(x))
        self.assertEqual(sum(range(1000)), int(self.db.get(b"a")))


class FullCounter(rocksdb.interfaces.MergeOperator):
    def name(self):
        return b"fullcounter"

    def full_merge(self, key, existing_value, operand_list):
        ret = sum([int(x) for x in operand_list])
        if existing_value:
            ret += int(existing_value)

        return (True, int_to_bytes(ret))

    def partial_merge(self, key, left, right):
        return (True, int_to_bytes(int(left) + int(right)))


class TestFullMerge(TestHelper):
    def setUp(self):
        TestHelper.setUp(self)
        opts = rocksdb.Options()
        opts.create_if_missing = True
        opts.merge_operator = FullCounter()
        self.db = rocksdb.DB(os.path.join(self.db_loc, "test"), opts)

    def test_merge(self):
        for x in range(1000):
            self.db.merge(b"a", int_to_bytes(x))
        self.assertEqual(sum(range(1000)), int(self.db.get(b"a")))


class SimpleComparator(rocksdb.interfaces.Comparator):
    def name(self):
        return b"mycompare"

    def compare(self, a, b):
        a = int(a)
        b = int(b)
        if a < b:
            return -1
        if a == b:
            return 0
        if a > b:
            return 1


class TestComparator(TestHelper):
    def setUp(self):
        TestHelper.setUp(self)
        opts = rocksdb.Options()
        opts.create_if_missing = True
        opts.comparator = SimpleComparator()
        self.db = rocksdb.DB(os.path.join(self.db_loc, "test"), opts)

    def test_compare(self):
        for x in range(1000):
            self.db.put(int_to_bytes(x), int_to_bytes(x))

        self.assertEqual(b"300", self.db.get(b"300"))


class StaticPrefix(rocksdb.interfaces.SliceTransform):
    def name(self):
        return b"static"

    def transform(self, src):
        return (0, 5)

    def in_domain(self, src):
        return len(src) >= 5

    def in_range(self, dst):
        return len(dst) == 5


class TestPrefixExtractor(TestHelper):
    def setUp(self):
        TestHelper.setUp(self)
        opts = rocksdb.Options(create_if_missing=True)
        opts.prefix_extractor = StaticPrefix()
        self.db = rocksdb.DB(os.path.join(self.db_loc, "test"), opts)

    def _fill_db(self):
        for x in range(3000):
            keyx = hex(x)[2:].zfill(5).encode("utf8") + b".x"
            keyy = hex(x)[2:].zfill(5).encode("utf8") + b".y"
            keyz = hex(x)[2:].zfill(5).encode("utf8") + b".z"
            self.db.put(keyx, b"x")
            self.db.put(keyy, b"y")
            self.db.put(keyz, b"z")

    def test_prefix_iterkeys(self):
        self._fill_db()
        self.assertEqual(b"x", self.db.get(b"00001.x"))
        self.assertEqual(b"y", self.db.get(b"00001.y"))
        self.assertEqual(b"z", self.db.get(b"00001.z"))

        it = self.db.iterkeys()
        it.seek(b"00002")

        ref = [b"00002.x", b"00002.y", b"00002.z"]
        ret = takewhile(lambda key: key.startswith(b"00002"), it)
        self.assertEqual(ref, list(ret))

    def test_prefix_iteritems(self):
        self._fill_db()

        it = self.db.iteritems()
        it.seek(b"00002")

        ref = {b"00002.z": b"z", b"00002.y": b"y", b"00002.x": b"x"}
        ret = takewhile(lambda item: item[0].startswith(b"00002"), it)
        self.assertEqual(ref, dict(ret))


class TestDBColumnFamilies(TestHelper):
    def setUp(self):
        TestHelper.setUp(self)
        opts = rocksdb.Options(create_if_missing=True)
        self.db = rocksdb.DB(os.path.join(self.db_loc, "test"), opts,)

        self.cf_a = self.db.create_column_family(b"A", rocksdb.ColumnFamilyOptions())
        self.cf_b = self.db.create_column_family(b"B", rocksdb.ColumnFamilyOptions())

    def test_column_families(self):
        families = self.db.column_families
        names = [handle.name for handle in families]
        self.assertEqual([b"default", b"A", b"B"], names)
        for name in names:
            self.assertIn(self.db.get_column_family(name), families)

        self.assertEqual(
            names,
            rocksdb.list_column_families(
                os.path.join(self.db_loc, "test"), rocksdb.Options(),
            ),
        )

    def test_get_none(self):
        self.assertIsNone(self.db.get(b"k"))
        self.assertIsNone(self.db.get((self.cf_a, b"k")))
        self.assertIsNone(self.db.get((self.cf_b, b"k")))

    def test_put_get(self):
        key = (self.cf_a, b"k")
        self.db.put(key, b"v")
        self.assertEqual(b"v", self.db.get(key))
        self.assertIsNone(self.db.get(b"k"))
        self.assertIsNone(self.db.get((self.cf_b, b"k")))

    def test_multi_get(self):
        data = [
            (b"a", b"1default"),
            (b"b", b"2default"),
            (b"c", b"3default"),
            ((self.cf_a, b"a"), b"1a"),
            ((self.cf_a, b"b"), b"2a"),
            ((self.cf_a, b"c"), b"3a"),
            ((self.cf_b, b"a"), b"1b"),
            ((self.cf_b, b"b"), b"2b"),
            ((self.cf_b, b"c"), b"3b"),
        ]
        for value in data:
            self.db.put(*value)

        multi_get_lookup = [value[0] for value in data]

        ret = self.db.multi_get(multi_get_lookup)
        ref = {value[0]: value[1] for value in data}
        self.assertEqual(ref, ret)

    def test_delete(self):
        self.db.put((self.cf_a, b"a"), b"b")
        self.assertEqual(b"b", self.db.get((self.cf_a, b"a")))
        self.db.delete((self.cf_a, b"a"))
        self.assertIsNone(self.db.get((self.cf_a, b"a")))

    def test_write_batch(self):
        cfa = self.db.get_column_family(b"A")
        batch = rocksdb.WriteBatch()
        batch.put((cfa, b"key"), b"v1")
        batch.delete((self.cf_a, b"key"))
        batch.put((cfa, b"key"), b"v2")
        batch.put((cfa, b"key"), b"v3")
        batch.put((cfa, b"a"), b"1")
        batch.put((cfa, b"b"), b"2")

        self.db.write(batch)
        query = [(cfa, b"key"), (cfa, b"a"), (cfa, b"b")]
        ret = self.db.multi_get(query)

        self.assertEqual(b"v3", ret[query[0]])
        self.assertEqual(b"1", ret[query[1]])
        self.assertEqual(b"2", ret[query[2]])

    def test_key_may_exists(self):
        self.db.put((self.cf_a, b"a"), b"1")

        self.assertEqual((False, None), self.db.key_may_exist((self.cf_a, b"x")))
        self.assertEqual(
            (False, None), self.db.key_may_exist((self.cf_a, b"x"), fetch=True)
        )
        self.assertEqual((True, None), self.db.key_may_exist((self.cf_a, b"a")))
        self.assertEqual(
            (True, b"1"), self.db.key_may_exist((self.cf_a, b"a"), fetch=True)
        )

    def test_iter_keys(self):
        for x in range(300):
            self.db.put((self.cf_a, int_to_bytes(x)), int_to_bytes(x))

        it = self.db.iterkeys(self.cf_a)
        self.assertEqual([], list(it))

        it.seek_to_last()
        self.assertEqual([(self.cf_a, b"99")], list(it))

        ref = sorted([(self.cf_a, int_to_bytes(x)) for x in range(300)])
        it.seek_to_first()
        self.assertEqual(ref, list(it))

        it.seek(b"90")
        ref = sorted([(self.cf_a, int_to_bytes(x)) for x in range(90, 100)])
        self.assertEqual(ref, list(it))

    def test_iter_values(self):
        for x in range(300):
            self.db.put((self.cf_b, int_to_bytes(x)), int_to_bytes(x * 1000))

        it = self.db.itervalues(self.cf_b)
        self.assertEqual([], list(it))

        it.seek_to_last()
        self.assertEqual([b"99000"], list(it))

        ref = sorted([int_to_bytes(x) for x in range(300)])
        ref = [int_to_bytes(int(x) * 1000) for x in ref]
        it.seek_to_first()
        self.assertEqual(ref, list(it))

        it.seek(b"90")
        ref = [int_to_bytes(x * 1000) for x in range(90, 100)]
        self.assertEqual(ref, list(it))

    def test_iter_items(self):
        for x in range(300):
            self.db.put((self.cf_b, int_to_bytes(x)), int_to_bytes(x * 1000))

        it = self.db.iteritems(self.cf_b)
        self.assertEqual([], list(it))

        it.seek_to_last()
        self.assertEqual([((self.cf_b, b"99"), b"99000")], list(it))

        ref = sorted([int_to_bytes(x) for x in range(300)])
        ref = [((self.cf_b, x), int_to_bytes(int(x) * 1000)) for x in ref]
        it.seek_to_first()
        self.assertEqual(ref, list(it))

        it.seek(b"90")
        ref = [
            ((self.cf_b, int_to_bytes(x)), int_to_bytes(x * 1000))
            for x in range(90, 100)
        ]
        self.assertEqual(ref, list(it))

    def test_reverse_iter(self):
        for x in range(100):
            self.db.put((self.cf_a, int_to_bytes(x)), int_to_bytes(x * 1000))

        it = self.db.iteritems(self.cf_a)
        it.seek_to_last()

        ref = reversed(sorted([(self.cf_a, int_to_bytes(x)) for x in range(100)]))
        ref = [(x, int_to_bytes(int(x[1]) * 1000)) for x in ref]

        self.assertEqual(ref, list(reversed(it)))

    def test_snapshot(self):
        cfa = self.db.get_column_family(b"A")
        self.db.put((cfa, b"a"), b"1")
        self.db.put((cfa, b"b"), b"2")

        snapshot = self.db.snapshot()
        self.db.put((cfa, b"a"), b"2")
        self.db.delete((cfa, b"b"))

        it = self.db.iteritems(cfa)
        it.seek_to_first()
        self.assertEqual({(cfa, b"a"): b"2"}, dict(it))

        it = self.db.iteritems(cfa, snapshot=snapshot)
        it.seek_to_first()
        self.assertEqual({(cfa, b"a"): b"1", (cfa, b"b"): b"2"}, dict(it))

    def test_get_property(self):
        secondary_location = os.path.join(self.db_loc, "secondary")
        cf = {b"A": rocksdb.ColumnFamilyOptions(), b"B": rocksdb.ColumnFamilyOptions()}
        secondary = rocksdb.DB(
            os.path.join(self.db_loc, "test"),
            rocksdb.Options(create_if_missing=True, max_open_files=-1),
            secondary_name=secondary_location,
            column_families=cf,
        )
        self.addCleanup(secondary.close)

        for x in range(300):
            x = int_to_bytes(x)
            self.db.put((self.cf_a, x), x)

        self.assertIsNone(self.db.get_property(b"does not exsits", self.cf_a))
        self.assertEqual(
            b"0",
            secondary.get_property(
                b"rocksdb.estimate-num-keys", secondary.get_column_family(b"A")
            ),
        )
        self.assertEqual(
            b"300", self.db.get_property(b"rocksdb.estimate-num-keys", self.cf_a)
        )

        secondary.try_catch_up_with_primary()

        self.assertEqual(
            b"300",
            secondary.get_property(
                b"rocksdb.estimate-num-keys", secondary.get_column_family(b"A")
            ),
        )
        self.assertEqual(
            b"300", self.db.get_property(b"rocksdb.estimate-num-keys", self.cf_a)
        )

    def test_compact_range(self):
        for x in range(10000):
            x = int_to_bytes(x)
            self.db.put((self.cf_b, x), x)

        self.db.compact_range(column_family=self.cf_b)


class OneCharacterPrefix(rocksdb.interfaces.SliceTransform):
    def name(self):
        return b"test prefix"

    def transform(self, src):
        return (0, 1)

    def in_domain(self, src):
        return len(src) >= 1

    def in_range(self, dst):
        return len(dst) == 1


class TestPrefixIterator(TestHelper):
    def setUp(self):
        TestHelper.setUp(self)
        opts = rocksdb.Options(create_if_missing=True)
        self.db = rocksdb.DB(os.path.join(self.db_loc, "test"), opts)

    def test_iterator(self):
        self.db.put(b"a0", b"a0_value")
        self.db.put(b"a1", b"a1_value")
        self.db.put(b"a1b", b"a1b_value")
        self.db.put(b"a2b", b"a2b_value")
        self.db.put(b"a3", b"a3_value")
        self.db.put(b"a4", b"a4_value")
        self.db.put(b"b0", b"b0_value")
        self.assertListEqual(
            [
                (b"a0", b"a0_value"),
                (b"a1", b"a1_value"),
                (b"a1b", b"a1b_value"),
                (b"a2b", b"a2b_value"),
                (b"a3", b"a3_value"),
                (b"a4", b"a4_value"),
            ],
            list(self.db.iterator(start=b"a", iterate_upper_bound=b"b")),
        )
        self.assertListEqual(
            [b"a0", b"a1", b"a1b", b"a2b", b"a3", b"a4"],
            list(
                self.db.iterator(
                    start=b"a", iterate_upper_bound=b"b", include_value=False
                )
            ),
        )
        self.assertListEqual(
            [b"a0", b"a1", b"a1b", b"a2b", b"a3", b"a4"],
            list(
                self.db.iterator(
                    start=b"a0", iterate_upper_bound=b"a5", include_value=False
                )
            ),
        )
        self.assertListEqual(
            [b"a4", b"a3", b"a2b", b"a1b", b"a1", b"a0"],
            list(
                reversed(
                    self.db.iterator(
                        start=b"a0", iterate_upper_bound=b"a5", include_value=False
                    )
                )
            ),
        )
        self.assertListEqual(
            [b"a0", b"a1", b"a1b", b"a2b", b"a3"],
            list(
                self.db.iterator(
                    start=b"a0", iterate_upper_bound=b"a4", include_value=False
                )
            ),
        )
        self.assertListEqual(
            [b"a0", b"a1", b"a1b"],
            list(
                self.db.iterator(
                    start=b"a0", iterate_upper_bound=b"a2", include_value=False
                )
            ),
        )
        self.assertListEqual(
            [b"a0", b"a1", b"a1b"],
            list(
                self.db.iterator(
                    start=b"a0", iterate_upper_bound=b"a2", include_value=False
                )
            ),
        )
        self.assertListEqual(
            [b"a1b", b"a1", b"a0"],
            list(
                reversed(
                    self.db.iterator(
                        start=b"a0", iterate_upper_bound=b"a2", include_value=False
                    )
                )
            ),
        )
        self.assertListEqual(
            [b"a0", b"a1", b"a1b", b"a2b", b"a3", b"a4"],
            list(
                self.db.iterator(
                    start=b"a", iterate_upper_bound=b"b0", include_value=False
                )
            ),
        )


class TestPrefixIteratorWithExtractor(TestHelper):
    def setUp(self):
        TestHelper.setUp(self)
        opts = rocksdb.Options(create_if_missing=True)
        opts.prefix_extractor = OneCharacterPrefix()
        self.db = rocksdb.DB(os.path.join(self.db_loc, "test"), opts)

    def test_iterator(self):
        self.db.put(b"a0", b"a0_value")
        self.db.put(b"a1", b"a1_value")
        self.db.put(b"a1b", b"a1b_value")
        self.db.put(b"a2b", b"a2b_value")
        self.db.put(b"a3", b"a3_value")
        self.db.put(b"a4", b"a4_value")
        self.db.put(b"b0", b"b0_value")
        self.assertListEqual(
            [
                (b"a0", b"a0_value"),
                (b"a1", b"a1_value"),
                (b"a1b", b"a1b_value"),
                (b"a2b", b"a2b_value"),
                (b"a3", b"a3_value"),
                (b"a4", b"a4_value"),
            ],
            list(self.db.iterator(start=b"a", prefix_same_as_start=True)),
        )
        self.assertListEqual(
            [b"a0", b"a1", b"a1b", b"a2b", b"a3", b"a4"],
            list(
                self.db.iterator(
                    start=b"a", include_value=False, prefix_same_as_start=True
                )
            ),
        )
        self.assertListEqual(
            [b"a0", b"a1", b"a1b", b"a2b", b"a3", b"a4"],
            list(
                self.db.iterator(
                    start=b"a0", iterate_upper_bound=b"a5", include_value=False
                )
            ),
        )
        self.assertListEqual(
            [b"a4", b"a3", b"a2b", b"a1b", b"a1", b"a0"],
            list(
                reversed(
                    self.db.iterator(
                        start=b"a0", iterate_upper_bound=b"a5", include_value=False
                    )
                )
            ),
        )
        self.assertListEqual(
            [b"a0", b"a1", b"a1b", b"a2b", b"a3"],
            list(
                self.db.iterator(
                    start=b"a0", iterate_upper_bound=b"a4", include_value=False
                )
            ),
        )
        self.assertListEqual(
            [b"a0", b"a1", b"a1b"],
            list(
                self.db.iterator(
                    start=b"a0", iterate_upper_bound=b"a2", include_value=False
                )
            ),
        )
        self.assertListEqual(
            [b"a0", b"a1", b"a1b"],
            list(
                self.db.iterator(
                    start=b"a0", iterate_upper_bound=b"a2", include_value=False
                )
            ),
        )
        self.assertListEqual(
            [b"a1b", b"a1", b"a0"],
            list(
                reversed(
                    self.db.iterator(
                        start=b"a0", iterate_upper_bound=b"a2", include_value=False
                    )
                )
            ),
        )
        self.assertListEqual(
            [b"a0", b"a1", b"a1b", b"a2b", b"a3", b"a4"],
            list(
                self.db.iterator(
                    start=b"a", iterate_upper_bound=b"b0", include_value=False
                )
            ),
        )

    def test_column_family_iterator(self):
        cf_a = self.db.create_column_family(b"first", rocksdb.ColumnFamilyOptions())
        cf_b = self.db.create_column_family(b"second", rocksdb.ColumnFamilyOptions())

        self.db.put((cf_a, b"a0"), b"a0_value")
        self.db.put((cf_a, b"a1"), b"a1_value")
        self.db.put((cf_a, b"a1b"), b"a1b_value")
        self.db.put((cf_a, b"a2b"), b"a2b_value")
        self.db.put((cf_a, b"a3"), b"a3_value")
        self.db.put((cf_a, b"a4"), b"a4_value")
        self.db.put((cf_b, b"b0"), b"b0_value")

        self.assertListEqual(
            [
                (b"a0", b"a0_value"),
                (b"a1", b"a1_value"),
                (b"a1b", b"a1b_value"),
                (b"a2b", b"a2b_value"),
                (b"a3", b"a3_value"),
                (b"a4", b"a4_value"),
            ],
            list(
                map(
                    lambda x: (x[0][-1], x[1]),
                    self.db.iterator(
                        column_family=cf_a, start=b"a", prefix_same_as_start=True
                    ),
                )
            ),
        )
        self.assertListEqual(
            [b"a0", b"a1", b"a1b", b"a2b", b"a3", b"a4"],
            list(
                map(
                    lambda x: x[-1],
                    self.db.iterator(
                        column_family=cf_a,
                        start=b"a",
                        include_value=False,
                        prefix_same_as_start=True,
                    ),
                )
            ),
        )
        self.assertListEqual(
            [b"a0", b"a1", b"a1b", b"a2b", b"a3", b"a4"],
            list(
                map(
                    lambda x: x[-1],
                    self.db.iterator(
                        column_family=cf_a,
                        start=b"a0",
                        iterate_upper_bound=b"a5",
                        include_value=False,
                    ),
                )
            ),
        )
        self.assertListEqual(
            [b"a4", b"a3", b"a2b", b"a1b", b"a1", b"a0"],
            list(
                map(
                    lambda x: x[-1],
                    reversed(
                        self.db.iterator(
                            column_family=cf_a,
                            start=b"a0",
                            iterate_upper_bound=b"a5",
                            include_value=False,
                        )
                    ),
                )
            ),
        )
        self.assertListEqual(
            [b"a0", b"a1", b"a1b", b"a2b", b"a3"],
            list(
                map(
                    lambda x: x[-1],
                    self.db.iterator(
                        column_family=cf_a,
                        start=b"a0",
                        iterate_upper_bound=b"a4",
                        include_value=False,
                    ),
                )
            ),
        )
        self.assertListEqual(
            [b"a0", b"a1", b"a1b"],
            list(
                map(
                    lambda x: x[-1],
                    self.db.iterator(
                        column_family=cf_a,
                        start=b"a0",
                        iterate_upper_bound=b"a2",
                        include_value=False,
                    ),
                )
            ),
        )
        self.assertListEqual(
            [b"a1b", b"a1", b"a0"],
            list(
                map(
                    lambda x: x[-1],
                    reversed(
                        self.db.iterator(
                            column_family=cf_a,
                            start=b"a0",
                            iterate_upper_bound=b"a2",
                            include_value=False,
                        )
                    ),
                )
            ),
        )
        self.assertListEqual(
            [b"b0"],
            list(
                map(
                    lambda x: x[-1],
                    self.db.iterator(
                        column_family=cf_b, start=b"b", include_value=False
                    ),
                )
            ),
        )
