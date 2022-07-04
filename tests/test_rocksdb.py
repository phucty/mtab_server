import gc
import os
import random
import shutil
import unittest
from mtab_server.resources.db.db_rocks import RocksDBWorker, DBSpec
from mtab_server.resources.db.utils import ToBytesType
from mtab_server.utils.benchmark import profile

TEMP_LIMIT = 1000
VALUE_LIMIT = 10000
TEMP_SCHEMA = {
    "db1": DBSpec("db1", integerkey=False, bytes_value=ToBytesType.OBJ),
    "db2": DBSpec("db2", integerkey=True, bytes_value=ToBytesType.OBJ),
    "db3": DBSpec("db3", integerkey=True, bytes_value=ToBytesType.INT_BITMAP),
    "db4": DBSpec("db4", integerkey=True, bytes_value=ToBytesType.INT_NUMPY),
    "db5": DBSpec(
        "db5", integerkey=False, bytes_value=ToBytesType.INT_NUMPY, prefix_len=5
    ),
    "db6": DBSpec(
        "db6", integerkey=False, bytes_value=ToBytesType.INT_BITMAP, prefix_len=5
    ),
}
TEMP_DATA = {
    "db1": {str(i): i for i in range(TEMP_LIMIT)},
    "db2": {i: str(i) for i in range(TEMP_LIMIT)},
    "db3": {i: list(range(random.randint(1, VALUE_LIMIT))) for i in range(TEMP_LIMIT)},
    "db4": {i: list(range(random.randint(1, VALUE_LIMIT))) for i in range(TEMP_LIMIT)},
    "db5": {
        f"{i}|{i*2}": list(range(random.randint(1, VALUE_LIMIT)))
        for i in range(TEMP_LIMIT)
    },
    "db6": {
        f"{i}|{i*2}": list(range(random.randint(1, VALUE_LIMIT)))
        for i in range(TEMP_LIMIT)
    },
}


class TestHelper(unittest.TestCase):
    def setUp(self):
        self.db_loc = "/tmp/mtab_server/rocksdb/"
        self.addCleanup(self._close_db)

    def _close_db(self):
        gc.collect()
        if os.path.exists(self.db_loc):
            shutil.rmtree(self.db_loc)


class TestRocksDB(TestHelper):
    def setUp(self):
        TestHelper.setUp(self)
        self.db_schema = TEMP_SCHEMA.values()

    def create_new_db(self, db_id=1):
        return RocksDBWorker(
            os.path.join(self.db_loc, str(db_id)),
            db_schema=self.db_schema,
            create_new=True,
        )

    @profile
    def test_basic_opts(self):
        db_write = self.create_new_db(1)
        truth = {b"default", b"db_size"}
        truth.update({f"db{i}".encode("utf-8") for i in range(1, 7)})
        columns_names = {handle.name for handle in db_write.env.column_families}
        self.assertEqual(truth, columns_names)

        # test_write_a sample
        # Test put key (str) : value (int)
        db_name = "db1"
        key, value = "99999", 99_999
        # Test put sample
        db_write.put(db_name, key, value)
        read_value = db_write.get_value(db_name, key)
        self.assertEqual(value, read_value)

        db_write.delete(db_name, key + "1")
        # Test delete sample
        db_write.delete(db_name, key)
        read_value = db_write.get_value(db_name, key)
        self.assertEqual(None, read_value)

        # Test put key (str) : value (int)
        db_name = "db3"
        key, value = 1, list(range(10))
        # Test put sample
        db_write.put(db_name, key, value)
        read_value = db_write.get_value(db_name, key)
        self.assertEqual(value, list(read_value))
        # Test extend value
        extend_value = list(range(10, 20))
        db_write.merge(db_name, key, extend_value)
        read_value = db_write.get_value(db_name, key)
        self.assertEqual(value + extend_value, list(read_value))
        # Test delete sample
        db_write.delete(db_name, key)
        read_value = db_write.get_value(db_name, key)
        self.assertEqual(None, read_value)

        # test_write_read_samples
        for data_name, data_samples in TEMP_DATA.items():
            db_write.put_batch(data_name, data_samples, show_progress=False)

        read_samples = {
            db_name: db_write.get_values(
                db_name, list(data_samples.keys()), to_list=True
            )
            for db_name, data_samples in TEMP_DATA.items()
        }
        for data_name, data_samples in TEMP_DATA.items():
            self.assertEqual(data_samples, read_samples.get(data_name))

        # Test iter
        iter_samples = {
            db_name: dict(db_write.iter_db(db_name, to_list=True))
            for db_name, data_samples in TEMP_DATA.items()
        }
        for data_name, data_samples in TEMP_DATA.items():
            self.assertEqual(data_samples, iter_samples.get(data_name))

        # Test iter prefix
        data_samples = {
            f"{i}|{i}": list(range(random.randint(1, TEMP_LIMIT)))
            for i in range(TEMP_LIMIT)
        }
        db_write.put_batch("db6", data_samples, show_progress=False)

        prefix_sample = dict(db_write.iter_db_prefix("db6", f"{11}|"))
        self.assertEqual({"11|22", "11|11"}, set(prefix_sample.keys()))


if __name__ == "__main__":
    unittest.main()
