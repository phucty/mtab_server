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


def step_1_download_dumps(
    ver_wd_json: str = cf.VER_WD_JSON,
    ver_wd_sql: str = cf.VER_WD_SQL,
    dir_wd: str = cf.DIR_DUMPS_WD,
    ver_wp: str = cf.VER_WP,
    dir_wp: str = cf.DIR_DUMPS_WP,
    dir_dp: str = cf.DIR_DUMPS_DP,
    print_status: bool = True,
):
    from mtab_server.utils import downloader

    start = time()
    # 1. Download Wikidata dump files
    downloader.download_dump_wikidata_json(
        json_ver=ver_wd_json,
        sql_ver=ver_wd_sql,
        download_dir=dir_wd,
        get_bz2=True,
        print_status=print_status,
    )
    # 2. Download Wikipedia dump file (English)
    downloader.download_dump_wikipedia(
        ver=ver_wp, download_dir=dir_wp, langs=["en"], print_status=print_status,
    )
    # # 3. Download DBpedia dump files
    # downloader.download_dump_dbpedia(download_dir=dir_dp)
    iw.print_status(f"Download dump files in {timedelta(seconds=time() - start)}")


def step_2_build_resources():
    from mtab_server.resources.db.db_wikidata import DBWikidata

    db = DBWikidata()
    db.build_trie_and_redirects()


# @profile
# def run_write(db_write):
#     for data_name, data_samples in TEMP_DATA.items():
#         db_write.put_batch(
#             data_name, data_samples, show_progress=True, check_exist=False
#         )
#
#
# @profile
# def run_seek(db_write):
#     read_samples = {
#         db_name: db_write.get_values(db_name, list(data_samples.keys()), to_list=True)
#         for db_name, data_samples in TEMP_DATA.items()
#     }
#
#
# @profile
# def run_iter(db_write):
#     iter_samples = {
#         db_name: dict(db_write.iter_db(db_name, to_list=True))
#         for db_name, data_samples in TEMP_DATA.items()
#     }


if __name__ == "__main__":
    # step_1_download_dumps()
    # step_2_build_resources()
    run_test_db()
