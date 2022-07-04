import random
from datetime import timedelta
from time import time

from mtab_server.utils import io_worker as iw
from config import config as cf
import gc
import os
import shutil
import unittest
import pytest
import tempfile
from mtab_server.resources.db.db_rocks import RocksDBWorker, DBSpec
from mtab_server.resources.db.utils import ToBytesType
from mtab_server.utils.benchmark import profile
from tests.test_rocksdb import TEMP_SCHEMA, TEMP_DATA, TEMP_LIMIT, VALUE_LIMIT


@profile
def run_test_db_write():
    db_loc = "/tmp/mtab_server/rocksdb/"
    if os.path.exists(db_loc):
        shutil.rmtree(db_loc)
    db_write = RocksDBWorker(
        os.path.join(db_loc, str(1)), db_schema=TEMP_SCHEMA.values(), create_new=True,
    )
    truth = {b"default", b"db_size"}
    truth.update({f"db{i}".encode("utf-8") for i in range(1, 7)})
    columns_names = {handle.name for handle in db_write.env.column_families}

    # test_write_a sample
    # Test put key (str) : value (int)
    db_name = "db1"
    key, value = "99999", 99_999
    # Test put sample
    db_write.put(db_name, key, value)
    read_value = db_write.get_value(db_name, key)

    db_write.delete(db_name, key + "1")
    # Test delete sample
    db_write.delete(db_name, key)
    read_value = db_write.get_value(db_name, key)

    # Test put key (str) : value (int)
    db_name = "db3"
    key, value = 1, list(range(10))
    # Test put sample
    db_write.put(db_name, key, value)
    read_value = db_write.get_value(db_name, key)

    # Test extend value
    extend_value = list(range(10, 20))
    db_write.merge(db_name, key, extend_value)
    read_value = db_write.get_value(db_name, key)

    # Test delete sample
    db_write.delete(db_name, key)
    read_value = db_write.get_value(db_name, key)

    # test_write_read_samples
    @profile
    def run_write():
        for data_name, data_samples in TEMP_DATA.items():
            db_write.put_batch(
                data_name, data_samples, show_progress=False, check_exist=False
            )

    run_write()

    @profile
    def run_seek():
        read_samples = {
            db_name: db_write.get_values(
                db_name, list(data_samples.keys()), to_list=True
            )
            for db_name, data_samples in TEMP_DATA.items()
        }

    run_seek()
    # Test iter
    @profile
    def run_iter():
        iter_samples = {
            db_name: dict(db_write.iter_db(db_name, to_list=True))
            for db_name, data_samples in TEMP_DATA.items()
        }

    run_iter()

    # Test iter prefix
    data_samples = {
        i: list(range(random.randint(1, VALUE_LIMIT))) for i in range(TEMP_LIMIT)
    }

    @profile
    def run_merge_batch(data_samples):
        data_samples = {f"{k}|{k*2}": v for k, v in data_samples.items()}
        db_write.merge_batch("db6", data_samples, show_progress=False, check_exist=True)

    run_merge_batch(data_samples)

    @profile
    def run_put_batch(data_samples):
        data_samples = {f"{k}|{k}": v for k, v in data_samples.items()}
        db_write.put_batch("db6", data_samples, show_progress=False, check_exist=True)

    run_put_batch(data_samples)

    @profile
    def run_seek_normal():
        db_name = "db6"
        data_samples = TEMP_DATA[db_name]
        read_samples = db_write.get_values(
            db_name, list(data_samples.keys()), to_list=True
        )

    run_seek_normal()

    @profile
    def run_seek_prefix():
        db_name = "db5"
        data_samples = TEMP_DATA[db_name]
        keys = [key.split("|")[0] + "|" for key in data_samples.keys()]

        read_samples = {
            key: dict(db_write.iter_db_prefix(db_name, key, to_list=True))
            for key in keys
        }

    run_seek_prefix()
    # normal_sample = db_write.get_value("db6", f"{11}|{11}")
    # prefix_sample = dict(db_write.iter_db_prefix("db6", f"{11}|"))
    print(db_write.view())
    # db_write.compact()
    # print(db_write.view())

    random_key = db_write.get_random_key("db6")
    random_value = db_write.get_random_value("db6")
    random_item = db_write.get_random_item("db6")
    head_items = db_write.head("db6", 5, from_i=2)
    available = db_write.is_available("db6", "11|11")
    available = db_write.is_available("db6", "11|12")
    # db_write.close()


@profile
def run_test_db_read():
    db_loc = "/tmp/mtab_server/rocksdb/"
    db_write = RocksDBWorker(os.path.join(db_loc, str(1)), read_only=True)
    truth = {b"default", b"db_size"}
    truth.update({f"db{i}".encode("utf-8") for i in range(1, 7)})
    columns_names = {handle.name for handle in db_write.env.column_families}

    @profile
    def run_seek():
        read_samples = {
            db_name: db_write.get_values(
                db_name, list(data_samples.keys()), to_list=True
            )
            for db_name, data_samples in TEMP_DATA.items()
        }

    run_seek()
    # Test iter
    @profile
    def run_iter():
        iter_samples = {
            db_name: dict(db_write.iter_db(db_name, to_list=True))
            for db_name, data_samples in TEMP_DATA.items()
        }

    run_iter()

    @profile
    def run_seek_normal():
        db_name = "db6"
        data_samples = TEMP_DATA[db_name]
        read_samples = db_write.get_values(
            db_name, list(data_samples.keys()), to_list=True
        )

    run_seek_normal()

    @profile
    def run_seek_prefix():
        db_name = "db5"
        data_samples = TEMP_DATA[db_name]
        keys = [key.split("|")[0] + "|" for key in data_samples.keys()]

        read_samples = {
            key: dict(db_write.iter_db_prefix(db_name, key, to_list=True))
            for key in keys
        }

    run_seek_prefix()
    print(db_write.view())

    random_key = db_write.get_random_key("db6")
    random_value = db_write.get_random_value("db6")
    random_item = db_write.get_random_item("db6")
    head_items = db_write.head("db6", 5, from_i=2)
    available = db_write.is_available("db6", "11|11")
    available = db_write.is_available("db6", "11|12")


if __name__ == "__main__":
    run_test_db_write()
    run_test_db_read()
