# import shutil
# from freaddb import config
# from freaddb.db_lmdb import DBSpec, FReadDB
#
#
# def test_db_basic():
#     data_file = "/tmp/mtab_server/db_test_basic"
#     shutil.rmtree(data_file, ignore_errors=True)
#     data_schema = [
#         DBSpec(
#             name="data0",
#             integerkey=False,
#             bytes_value=config.ToBytesType.OBJ,
#             compress_value=True,
#         ),
#         DBSpec(name="data1", integerkey=True, bytes_value=config.ToBytesType.OBJ),
#         DBSpec(name="data2", integerkey=False, bytes_value=config.ToBytesType.PICKLE),
#         DBSpec(name="data3", integerkey=False, bytes_value=config.ToBytesType.BYTES),
#         DBSpec(name="data4", integerkey=True, bytes_value=config.ToBytesType.INT_NUMPY),
#         DBSpec(
#             name="data5", integerkey=True, bytes_value=config.ToBytesType.INT_BITMAP
#         ),
#     ]
#     data = {
#         "data0": {"One": {1: "One"}, "Two": {2: "Two"}},
#         "data1": {1: "One", 2: "Two"},
#         "data2": {"One": 1, "Two": 2},
#         "data3": {"One": b"1", "Two": b"2"},
#         "data4": {i: list(range(i * 10)) for i in range(10, 20)},
#         "data5": {i: list(range(i * 10)) for i in range(10, 20)},
#     }
#     to_list_data = {"data4", "data5"}
#
#     db = FReadDB(db_file=data_file, db_schema=data_schema, buff_limit=config.SIZE_1GB)
#     for data_name, data_items in data.items():
#         for key, value in data_items.items():
#             db.add(data_name, key, value)
#     db.save_buff()
#
#     # Get a key
#     sample = db.get_value("data1", 1)
#     assert sample == "One"
#
#     # Get many keys
#     for data_name, data_samples in data.items():
#         sample = db.get_values(data_name, list(data_samples.keys()))
#         if data_name in to_list_data:
#             sample = {k: list(v) for k, v in sample.items()}
#         assert sample == data_samples
