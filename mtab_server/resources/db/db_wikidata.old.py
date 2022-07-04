# import bz2
# import csv
# import gc
# import gzip
# import os.path
# import queue
# import shutil
# from collections import defaultdict
#
# import ujson
# from freaddb.db_lmdb import DBSpec, FReadDB
# from freaddb.config import ToBytesType
# from pyroaring import BitMap
# from tqdm import tqdm
#
# from mtab_server.utils import io_worker as iw, downloader
# from config import config as cf
#
#
# def parse_sql_values(line):
#     values = line[line.find("` VALUES ") + 9 :]
#     latest_row = []
#     reader = csv.reader(
#         [values],
#         delimiter=",",
#         doublequote=False,
#         escapechar="\\",
#         quotechar="'",
#         strict=True,
#     )
#     for reader_row in reader:
#         for column in reader_row:
#             if len(column) == 0 or column == "NULL":
#                 latest_row.append(chr(0))
#                 continue
#             if column[0] == "(":
#                 new_row = False
#                 if len(latest_row) > 0:
#                     if latest_row[-1][-1] == ")":
#                         latest_row[-1] = latest_row[-1][:-1]
#                         new_row = True
#                 if new_row:
#                     yield latest_row
#                     latest_row = []
#                 if len(latest_row) == 0:
#                     column = column[1:]
#             latest_row.append(column)
#         if latest_row[-1][-2:] == ");":
#             latest_row[-1] = latest_row[-1][:-2]
#             yield latest_row
#
#
# def convert_num(text):
#     if not text:
#         return None
#     try:
#         return float(text)
#     except ValueError:
#         return None
#
#
# def get_wd_int(wd_id):
#     result = None
#     if wd_id and len(wd_id) and wd_id[0].lower() in ["p", "q"] and " " not in wd_id:
#         result = convert_num(wd_id[1:])
#     return result
#
#
# def is_wd_item(wd_id):
#     if get_wd_int(wd_id) is None:
#         return False
#     else:
#         return True
#
#
# def reader_wikidata_dump(dir_dump):
#     if ".bz2" in dir_dump:
#         reader = bz2.BZ2File(dir_dump)
#     elif ".gz" in dir_dump:
#         reader = gzip.open(dir_dump, "rt")
#     else:
#         reader = open(dir_dump)
#
#     if reader:
#         for line in reader:
#             yield line
#         reader.close()
#
#
# def parse_json_dump(json_line):
#     if isinstance(json_line, bytes) or isinstance(json_line, bytearray):
#         line = json_line.rstrip().decode(cf.ENCODING)
#     else:
#         line = json_line.rstrip()
#     if line in ("[", "]"):
#         return None
#
#     if line[-1] == ",":
#         line = line[:-1]
#     try:
#         obj = ujson.loads(line)
#     except ValueError:
#         return None
#     if obj["type"] != "item" and is_wd_item(obj["id"]) is False:
#         return None
#
#     wd_id = obj["id"]
#     wd_obj = {}
#
#     def update_dict(attribute, attr_value):
#         if attribute == "aliases":
#             wd_obj[attribute] = {
#                 lang: {v.get(attr_value) for v in value}
#                 for lang, value in obj.get(attribute, {}).items()
#             }
#
#         else:
#             wd_obj[attribute] = {
#                 lang: value.get(attr_value)
#                 for lang, value in obj.get(attribute, {}).items()
#             }
#
#     update_dict(attribute="labels", attr_value="value")
#     update_dict(attribute="descriptions", attr_value="value")
#     update_dict(attribute="sitelinks", attr_value="title")
#     update_dict(attribute="aliases", attr_value="value")
#
#     # Get english label:
#     wd_obj["label"] = wd_obj.get("labels", {}).get("en", wd_id)
#
#     # Statements
#     if obj.get("claims"):
#         for prop, claims in obj["claims"].items():
#             if wd_obj.get("claims") is None:
#                 wd_obj["claims"] = defaultdict()
#
#             # if wd_obj.get("claims_provenance") is None:
#             #     wd_obj["claims_provenance"] = defaultdict()
#
#             for claim in claims:
#                 if (
#                     claim.get("mainsnak") is None
#                     or claim["mainsnak"].get("datavalue") is None
#                 ):
#                     continue
#                 claim_type = claim["mainsnak"]["datavalue"]["type"]
#                 claim_value = claim["mainsnak"]["datavalue"]["value"]
#
#                 claim_references = claim.get("references")
#                 if claim_references:
#                     nodes = []
#                     for reference_node in claim_references:
#                         if not reference_node.get("snaks"):
#                             continue
#                         node = {}
#                         for ref_prop, ref_claims in reference_node["snaks"].items():
#                             for ref_claim in ref_claims:
#                                 if ref_claim.get("datavalue") is None:
#                                     continue
#                                 ref_type = ref_claim["datavalue"]["type"]
#                                 ref_value = ref_claim["datavalue"]["value"]
#                                 if node.get(ref_type) is None:
#                                     node[ref_type] = defaultdict(list)
#
#                                 if ref_type == "wikibase-entityid":
#                                     ref_value = ref_value["id"]
#                                 elif ref_type == "time":
#                                     ref_value = ref_value["time"]
#                                     ref_value = ref_value.replace("T00:00:00Z", "")
#                                     if ref_value[0] == "+":
#                                         ref_value = ref_value[1:]
#                                 elif ref_type == "quantity":
#                                     ref_unit = ref_value["unit"]
#                                     ref_unit = ref_unit.replace(cf.WD, "")
#                                     ref_value = ref_value["amount"]
#                                     if ref_value[0] == "+":
#                                         ref_value = ref_value[1:]
#                                     ref_value = (ref_value, ref_unit)
#                                 elif ref_type == "monolingualtext":
#                                     ref_value = ref_value["text"]
#
#                                 node[ref_type][ref_prop].append(ref_value)
#                         nodes.append(node)
#                     claim_references = nodes
#                 else:
#                     claim_references = []
#
#                 if wd_obj["claims"].get(claim_type) is None:
#                     wd_obj["claims"][claim_type] = defaultdict(list)
#
#                 # if wd_obj["claims_provenance"].get(claim_type) is None:
#                 #     wd_obj["claims_provenance"][claim_type] = defaultdict(list)
#
#                 if claim_type == "wikibase-entityid":
#                     claim_value = claim_value["id"]
#                 elif claim_type == "time":
#                     claim_value = claim_value["time"]
#                     claim_value = claim_value.replace("T00:00:00Z", "")
#                     if claim_value[0] == "+":
#                         claim_value = claim_value[1:]
#                 elif claim_type == "quantity":
#                     claim_unit = claim_value["unit"]
#                     claim_unit = claim_unit.replace(cf.WD, "")
#                     claim_value = claim_value["amount"]
#                     if claim_value[0] == "+":
#                         claim_value = claim_value[1:]
#                     claim_value = (claim_value, claim_unit)
#                 elif claim_type == "monolingualtext":
#                     claim_value = claim_value["text"]
#
#                 wd_obj["claims"][claim_type][prop].append(
#                     {"value": claim_value, "references": claim_references}
#                 )
#                 # wd_obj["claims"][claim_type][prop].append(claim_value)
#                 # wd_obj["claims_provenance"][claim_type][prop].append(
#                 #     {"value": claim_value, "provenance": claim_references}
#                 # )
#
#     return wd_id, wd_obj
#
#
# class DBWikidata:
#     def __init__(
#         self,
#         db_file: str = cf.DIR_WIKIDB,
#         readonly=True,
#         buff_limit=cf.SIZE_1GB * 10,
#         create_new=False,
#     ):
#         self.db = self.init_db(db_file, readonly, buff_limit, create_new)
#
#     @staticmethod
#     def init_db(data_file, readonly, buff_limit, create_new):
#         if create_new:
#             iw.delete_file(data_file)
#             iw.delete_file(data_file + ".json")
#
#         if os.path.exists(data_file):
#             return FReadDB(db_file=data_file, buff_limit=buff_limit, readonly=readonly)
#
#         data_schema = [
#             # "Q123": 1
#             DBSpec(
#                 name="wdid_to_lid",
#                 integerkey=False,
#                 bytes_value=ToBytesType.OBJ,
#                 compress_value=False,
#             ),
#             # 1: "Q123"
#             DBSpec(
#                 name="lid_to_wdid",
#                 integerkey=True,
#                 bytes_value=ToBytesType.OBJ,
#                 compress_value=False,
#             ),
#             # 1: 2
#             # 3: 2
#             DBSpec(
#                 name="redirect",
#                 integerkey=True,
#                 bytes_value=ToBytesType.OBJ,
#                 compress_value=False,
#             ),
#             # 2: [1, 3]
#             DBSpec(
#                 name="redirect_of",
#                 integerkey=True,
#                 bytes_value=ToBytesType.INT_NUMPY,
#                 compress_value=False,
#             ),
#             # 1: "label"
#             DBSpec(
#                 name="label",
#                 integerkey=True,
#                 bytes_value=ToBytesType.OBJ,
#                 compress_value=False,
#             ),
#             # 1: {"en": "label"}
#             DBSpec(
#                 name="label_langs",
#                 integerkey=True,
#                 bytes_value=ToBytesType.OBJ,
#                 compress_value=True,
#             ),
#             # 1: {"en", ["name1", "name2"]}
#             DBSpec(
#                 name="aliases_langs",
#                 integerkey=True,
#                 bytes_value=ToBytesType.OBJ,
#                 compress_value=True,
#             ),
#             DBSpec(
#                 name="descriptions_langs",
#                 integerkey=True,
#                 bytes_value=ToBytesType.OBJ,
#                 compress_value=True,
#             ),
#             DBSpec(
#                 name="claims_ent",
#                 integerkey=False,
#                 bytes_value=ToBytesType.INT_BITMAP,
#                 compress_value=False,
#             ),
#             DBSpec(
#                 name="claims_ent_inv",
#                 integerkey=True,
#                 bytes_value=ToBytesType.INT_BITMAP,
#                 compress_value=False,
#             ),
#             DBSpec(
#                 name="claims_lit",
#                 integerkey=False,
#                 bytes_value=ToBytesType.OBJ,
#                 compress_value=True,
#             ),
#             DBSpec(
#                 name="wikipedia",
#                 integerkey=True,
#                 bytes_value=ToBytesType.OBJ,
#                 compress_value=False,
#             ),
#             DBSpec(
#                 name="dbpedia",
#                 integerkey=True,
#                 bytes_value=ToBytesType.OBJ,
#                 compress_value=False,
#             ),
#             DBSpec(
#                 name="pagerank",
#                 integerkey=True,
#                 bytes_value=ToBytesType.OBJ,
#                 compress_value=False,
#             ),
#             # 1: ["name"]
#             DBSpec(
#                 name="name_en",
#                 integerkey=True,
#                 bytes_value=ToBytesType.OBJ,
#                 compress_value=True,
#             ),
#             # 1: ["name"] multilingual
#             DBSpec(
#                 name="name_all",
#                 integerkey=True,
#                 bytes_value=ToBytesType.OBJ,
#                 compress_value=False,
#             ),
#             # 1: {"Value": [property]}
#             DBSpec(
#                 name="values_ent",
#                 integerkey=True,
#                 bytes_value=ToBytesType.OBJ,
#                 compress_value=True,
#             ),
#             # 2: {"Value": {datatype: [property]}}
#             DBSpec(
#                 name="values_lit",
#                 integerkey=True,
#                 bytes_value=ToBytesType.OBJ,
#                 compress_value=True,
#             ),
#         ]
#
#         return FReadDB(
#             db_file=data_file,
#             db_schema=data_schema,
#             buff_limit=buff_limit,
#             readonly=readonly,
#         )
#
#     def get_redirect_of(self, wd_id, decode=True):
#         return self._get_db_item(
#             self.db_redirect_of,
#             wd_id,
#             bytes_value=ToBytesType.INT_NUMPY,
#             integerkey=True,
#             decode=decode,
#         )
#
#     def get_redirect(self, wd_id, decode=True):
#         return self._get_db_item(
#             self.db_redirect,
#             wd_id,
#             compress_value=False,
#             integerkey=True,
#             decode=decode,
#             get_redirect=False,
#         )
#
#     def keys(self):
#         for k in self.db_qid_trie:
#             yield k
#
#     def items(self):
#         for k in self.keys():
#             v = self.get_item(k)
#             yield k, v
#
#     def size(self):
#         return len(self.db_qid_trie)
#
#     def _get_db_item(
#         self,
#         db,
#         wd_id,
#         compress_value=False,
#         integerkey=True,
#         decode=True,
#         bytes_value=ToBytesType.OBJ,
#         lang=None,
#         get_redirect=True,
#     ):
#         if wd_id is None:
#             return None
#         if integerkey and not isinstance(wd_id, int):
#             wd_id = self.get_lid(wd_id)
#             if wd_id is None:
#                 return None
#         results = self.get_value(
#             db,
#             wd_id,
#             integerkey=integerkey,
#             compress_value=compress_value,
#             bytes_value=bytes_value,
#         )
#         if not results and get_redirect:
#             # Try redirect item
#             try:
#                 wd_id_redirect = self.get_redirect(wd_id, decode=False)
#                 if wd_id_redirect and wd_id_redirect != wd_id:
#                     results = self.get_value(
#                         db,
#                         wd_id_redirect,
#                         integerkey=integerkey,
#                         compress_value=compress_value,
#                         bytes_value=bytes_value,
#                     )
#             except Exception as message:
#                 iw.print_status(message, is_screen=False)
#         if not results:
#             return results
#         if lang and results and isinstance(results, dict):
#             results = results.get(lang)
#
#         if not decode:
#             return results
#         if decode and isinstance(results, int):
#             return self.db_qid_trie.restore_key(results)
#         if decode and type(results) in [list]:
#             return [self.db_qid_trie.restore_key(r) for r in results]
#
#         decode_results = {}
#         for c_type, c_statements in results.items():
#             decode_c_type = {}
#             for c_prop, c_values in c_statements.items():
#                 decode_c_prop = self.get_qid(c_prop)
#                 decode_c_values = []
#                 for c_value in c_values:
#                     decode_c_value = c_value
#                     if c_type == "wikibase-entityid":
#                         decode_c_value = self.get_qid(decode_c_value)
#                     elif c_type == "quantity":
#                         if decode_c_value[1] == -1:
#                             decode_c_value = (decode_c_value[0], 1)
#                         else:
#                             decode_c_value = (
#                                 decode_c_value[0],
#                                 self.get_qid(decode_c_value[1]),
#                             )
#                     decode_c_values.append(decode_c_value)
#                 decode_c_type[decode_c_prop] = decode_c_values
#
#             decode_results[c_type] = decode_c_type
#         return decode_results
#
#     def get_label(self, wd_id):
#         return self._get_db_item(
#             self.db_label, wd_id, compress_value=False, integerkey=True, decode=False
#         )
#
#     def get_labels(self, wd_id, lang=None):
#         return self._get_db_item(
#             self.db_labels,
#             wd_id,
#             compress_value=True,
#             integerkey=True,
#             decode=False,
#             lang=lang,
#         )
#
#     def get_descriptions(self, wd_id, lang=None):
#         return self._get_db_item(
#             self.db_descriptions,
#             wd_id,
#             compress_value=True,
#             integerkey=True,
#             decode=False,
#             lang=lang,
#         )
#
#     def get_aliases(self, wd_id, lang=None):
#         return self._get_db_item(
#             self.db_aliases,
#             wd_id,
#             compress_value=True,
#             integerkey=True,
#             decode=False,
#             lang=lang,
#         )
#
#     def get_sitelinks(self, wd_id):
#         return self._get_db_item(
#             self.db_sitelinks,
#             wd_id,
#             compress_value=True,
#             integerkey=True,
#             decode=False,
#         )
#
#     def get_claims(self, wd_id, get_qid=True):
#         return self._get_db_item(
#             self.db_claims, wd_id, compress_value=True, integerkey=True, decode=get_qid
#         )
#
#     def get_claims_ent(self, wd_id, prop_id=None, get_qid=True):
#         if not isinstance(wd_id, int):
#             wd_id = self.get_lid(wd_id)
#             if wd_id is None:
#                 return None
#
#         if prop_id is not None:
#             if not isinstance(prop_id, int):
#                 prop_id = self.get_lid(prop_id)
#                 if prop_id is None:
#                     return None
#             results = self.get_value(
#                 self.db_claims_ent,
#                 f"{wd_id}|{prop_id}",
#                 bytes_value=ToBytesType.INT_NUMPY,
#             )
#             if results and get_qid:
#                 results = [self.db_qid_trie.restore_key(r) for r in results]
#         else:
#             results = {}
#             for key, value in self.get_iter_with_prefix(
#                 self.db_claims_ent,
#                 f"{wd_id}|",
#                 bytes_value=ToBytesType.INT_NUMPY,
#                 get_values=True,
#             ):
#                 key = int(key.split("|")[-1])
#                 if get_qid:
#                     key = self.db_qid_trie.restore_key(key)
#                     value = [self.db_qid_trie.restore_key(v) for v in value]
#                 results[key] = value
#         return results
#
#     def get_item(self, wd_id):
#         result = dict()
#         result["wikidata_id"] = wd_id
#         if not isinstance(wd_id, int):
#             wd_id = self.get_lid(wd_id)
#             if wd_id is None:
#                 return None
#
#         def update_dict(attr, func):
#             tmp = func(wd_id)
#             if tmp is not None:
#                 result[attr] = tmp
#
#         wd_redirect = self.get_redirect(wd_id)
#         if wd_redirect and wd_redirect != wd_id:
#             result["wikidata_id"] = wd_redirect
#
#         update_dict("label", self.get_label)
#         update_dict("labels", self.get_labels)
#         update_dict("descriptions", self.get_descriptions)
#         update_dict("aliases", self.get_aliases)
#         update_dict("sitelinks", self.get_sitelinks)
#         update_dict("claims", self.get_claims)
#         update_dict("claims_ent", self.get_claims_ent)
#         return result
#
#     def get_instance_of(self, wd_id):
#         return self.get_claims_ent(wd_id, "P31")
#
#     def get_subclass_of(self, wd_id):
#         return self.get_claims_ent(wd_id, "P279")
#
#     def get_all_types(self, wd_id):
#         # wdt:P31/wdt:P279*
#         results = set()
#         p_items = self.get_instance_of(wd_id)
#         if p_items:
#             process_queue = queue.Queue()
#             for p_item in p_items:
#                 process_queue.put(p_item)
#             while process_queue.qsize():
#                 process_wd = process_queue.get()
#                 results.add(process_wd)
#                 p_items = self.get_subclass_of(process_wd)
#                 if p_items:
#                     for item in p_items:
#                         if item not in results:
#                             process_queue.put(item)
#         return list(results)
#
#     def get_wikipedia_title(self, lang, wd_id):
#         tmp = self.get_sitelinks(wd_id)
#         if tmp and tmp.get(f"{lang}wiki"):
#             return tmp[f"{lang}wiki"]
#         return None
#
#     def get_wikipedia_link(self, lang, wd_id):
#         title = self.get_wikipedia_title(lang, wd_id)
#         if title and isinstance(title, str):
#             title = title.replace(" ", "_")
#             return f"https://{lang}.wikipedia.org/wiki/{title}"
#         return None
#
#     def get_lid(self, wd_id, default=None):
#         results = self.db_qid_trie.get(wd_id)
#         if results is None:
#             return default
#         return results
#
#     def get_qid(self, lid):
#         if isinstance(lid, int):
#             results = self.db_qid_trie.restore_key(lid)
#             if results is not None:
#                 return results
#         return lid
#
#     def get_haswbstatements(self, statements, get_qid=True):
#         results = None
#         # sort attr
#         if statements:
#             sorted_attr = []
#             for operation, pid, qid in statements:
#                 fre = None
#                 if pid and qid:
#                     fre = self.get_head_qid(qid, pid, get_posting=False, get_qid=False)
#                 elif qid:
#                     fre = self.get_head_qid(qid, get_posting=False, get_qid=False)
#                 if fre is None:
#                     continue
#                 sorted_attr.append([operation, pid, qid, fre])
#             sorted_attr.sort(key=lambda x: x[3])
#             statements = [
#                 [operation, pid, qid] for operation, pid, qid, f in sorted_attr
#             ]
#
#         for operation, pid, qid in statements:
#             if pid and qid:
#                 tmp = self.get_head_qid(qid, pid, get_qid=False)
#             elif qid:
#                 tmp = self.get_head_qid(qid, get_qid=False)
#             else:
#                 tmp = BitMap()
#
#             if results is None:
#                 results = tmp
#                 if tmp is None:
#                     break
#             else:
#                 if operation == cf.ATTR_OPTS.AND:
#                     results = results & tmp
#                 elif operation == cf.ATTR_OPTS.OR:
#                     results = results | tmp
#                 elif operation == cf.ATTR_OPTS.NOT:
#                     results = BitMap.difference(results, tmp)
#                 else:  # default = AND
#                     results = results & tmp
#
#             # iw.print_status(
#             #     f"  {operation}. {pid}={qid} ({self.get_label(pid)}={self.get_label(qid)}) : {len(tmp):,} --> Context: {len(results):,}"
#             # )
#         if results is None:
#             return []
#         if get_qid:
#             results = [self.get_qid(i) for i in results]
#         else:
#             results = results.to_array()
#         return results
#
#     def build_wikipedia_wikidata_lid(self, step=10000):
#         dict_dir = f"{cf.DIR_MODELS}/wp2wd_lid/"
#         iw.create_dir(dict_dir)
#
#         wp2wd = defaultdict(dict)
#         n_articles = 0
#
#         def update_desc():
#             return f"{len(wp2wd):,} languages, {n_articles:,} articles"
#
#         p_bar = tqdm(total=self.size(), desc=update_desc())
#
#         for i, k in enumerate(self.keys()):
#             if i and i % step == 0:
#                 p_bar.update(step)
#                 p_bar.set_description(update_desc())
#             lid = self.get_lid(k)
#             links = self.get_sitelinks(k)
#             if not links:
#                 continue
#             for lang, title in links.items():
#                 if not lang.endswith("wiki"):
#                     continue
#                 lang = lang.replace("wiki", "")
#                 n_articles += 1
#                 wp2wd[lang][title] = lid
#         for lang, wp2wd_obj in wp2wd.items():
#             iw.save_obj_pkl(f"{dict_dir}/{lang}.pkl", wp2wd_obj)
#
#     def build(self):
#         # 1. Build trie and redirect
#         self.build_trie_and_redirects()
#
#         # 2. Build json dump
#         self.build_from_json_dump(n_process=6)
#
#         # 3. Build haswdstatement (Optional)
#         self.build_haswbstatements()
#
#         # 4. Post-processing to reduce file size
#         self.copy_lmdb()
#
#     def get_head_qid(self, tail_qid, pid=None, get_posting=True, get_qid=False):
#         if not isinstance(tail_qid, int):
#             tail_qid = self.get_lid(tail_qid)
#             if tail_qid is None:
#                 return None
#
#         if pid and not isinstance(pid, int):
#             pid = self.get_lid(pid)
#             if pid is None:
#                 return None
#
#         if not pid:
#             key = str(tail_qid)
#         else:
#             key = f"{tail_qid}|{pid}"
#
#         if not get_posting:
#             return self.get_memory_size(self.db_claim_ent_inv, key)
#
#         posting = self.get_value(
#             self.db_claim_ent_inv, key, bytes_value=ToBytesType.INT_BITMAP
#         )
#         if get_qid:
#             posting = [self.get_qid(p) for p in posting]
#         return posting
#
#     def build_haswbstatements(self, buff_limit=cf.SIZE_512MB, step=10000):
#         invert_index = defaultdict(BitMap)
#
#         for i, head_id in enumerate(tqdm(self.keys(), total=self.size())):
#             # if i and i % step == 0:
#             #     break
#
#             head_lid = self.get_lid(head_id)
#             if head_lid is None or not isinstance(head_lid, int):
#                 continue
#             v = self.get_claims_ent(head_lid, get_qid=False)
#             if not v:
#                 continue
#             for claim_prop, claim_value_objs in v.items():
#                 for claim_value_obj in claim_value_objs:
#                     claim_value = claim_value_obj
#                     if isinstance(claim_value, int):
#                         invert_index[f"{claim_value}|{claim_prop}"].add(head_lid)
#
#         invert_index = sorted(invert_index.items(), key=lambda x: x[0])
#         buff = []
#         buff_size = 0
#         tail_kv_list = []
#         tail_k = None
#         tail_v = BitMap()
#         for k, v in tqdm(invert_index, desc="Save db", total=len(invert_index)):
#             tmp_k = k.split("|")[0]
#             if tmp_k != tail_k:
#                 if tail_k:
#                     tail_k, tail_v = db_core.serialize(
#                         tail_k, tail_v, bytes_value=ToBytesType.INT_BITMAP
#                     )
#                     buff_size += len(tail_k) + len(tail_v)
#                     buff.append((tail_k, tail_v))
#                     buff.extend(tail_kv_list)
#                 tail_k = tmp_k
#                 tail_v = BitMap()
#                 tail_kv_list = []
#             tail_v.update(v)
#             k, v = db_core.serialize(k, v, bytes_value=ToBytesType.INT_BITMAP)
#             buff_size += len(k) + len(v)
#             tail_kv_list.append((k, v))
#             if buff_size > buff_limit:
#                 self.write_bulk(self.env, self.db_claim_ent_inv, buff, sort_key=False)
#                 buff = []
#                 buff_size = 0
#         if buff_size:
#             self.write_bulk(self.env, self.db_claim_ent_inv, buff, sort_key=False)
#
#
#     def build_index
#
#     def build_trie_and_redirects(self, step=100000):
#         if not os.path.exists(cf.DIR_DUMP_WD_PAGE) or not os.path.exists(
#             cf.DIR_DUMP_WD_REDIRECT
#         ):
#             downloader.download_dump_wikidata_json(
#                 json_ver=cf.VER_WD_JSON,
#                 sql_ver=cf.VER_WD_SQL,
#                 download_dir=cf.DIR_DUMPS_WD,
#                 get_bz2=True,
#                 print_status=True,
#             )
#
#         wd_id_qid = {}
#         with gzip.open(cf.DIR_DUMP_WD_PAGE, "rt", encoding="utf-8", newline="\n") as f:
#             p_bar = tqdm(desc="Wikidata pages")
#             i = 0
#             for line in f:
#                 if not line.startswith("INSERT INTO"):
#                     continue
#                 for v in parse_sql_values(line):
#                     if is_wd_item(v[2]):
#                         i += 1
#                         if i and i % step == 0:
#                             p_bar.update(step)
#                         wd_id_qid[v[0]] = v[2]
#                 # if len(wd_id_qid) > 1000000:
#                 #     break
#             p_bar.close()
#
#
#
#         self.db_qid_trie = marisa_trie.Trie(wd_id_qid.values())
#         self.db_qid_trie.save(cf.DIR_WIKIDATA_ITEMS_TRIE)
#         iw.print_status(f"Trie Saved: {len(self.db_qid_trie):,}")
#
#         buff_obj = defaultdict()
#         with gzip.open(
#             cf.DIR_DUMP_WD_REDIRECT, "rt", encoding="utf-8", newline="\n",
#         ) as f:
#             i = 0
#             p_bar = tqdm(desc="Wikidata redirects")
#             for line in f:
#                 if not line.startswith("INSERT INTO"):
#                     continue
#                 for v in parse_sql_values(line):
#                     qid = wd_id_qid.get(v[0])
#                     if not qid:
#                         continue
#                     lid = self.get_lid(qid)
#                     if lid is None:
#                         continue
#                     if is_wd_item(v[2]):
#                         redirect = self.get_lid(v[2])
#                         if redirect is None:
#                             continue
#                         buff_obj[lid] = redirect
#                         i += 1
#                         if i and i % step == 0:
#                             p_bar.update(step)
#             p_bar.close()
#
#         if buff_obj:
#             self.write_bulk(self.env, self.db_redirect, buff_obj, integerkey=True)
#             buff_obj_inv = defaultdict(set)
#             for k, v in buff_obj.items():
#                 buff_obj_inv[v].add(k)
#             self.write_bulk(
#                 self.env,
#                 self.db_redirect_of,
#                 buff_obj_inv,
#                 integerkey=True,
#                 bytes_value=ToBytesType.INT_NUMPY,
#             )
#
#     def build_from_json_dump(self, json_dump=cf.DIR_DUMP_WD, n_process=1, step=1000):
#         attr_db = {
#             "label": self.db_label,
#             "labels": self.db_labels,
#             "descriptions": self.db_descriptions,
#             "aliases": self.db_aliases,
#             "claims": self.db_claims,
#             "claims_ent": self.db_claims_ent,
#             "sitelinks": self.db_sitelinks,
#         }
#
#         buff = {attr: [] for attr in attr_db.keys()}
#         buff_size = 0
#         count = 0
#
#         def update_desc():
#             return f"Wikidata Parsing|items:{count:,}|{buff_size / cf.LMDB_BUFF_BYTES_SIZE * 100:.0f}%"
#
#         def save_buff(buff):
#             for attr in buff.keys():
#                 if not buff[attr]:
#                     continue
#                 buff[attr].sort(key=lambda x: x[0])
#                 if attr != "claims_ent":
#                     buff[attr] = [
#                         (db_core.serialize_key(k, integerkey=True), v)
#                         for k, v in buff[attr]
#                     ]
#
#                 self.write_bulk(self.env, attr_db[attr], buff[attr], sort_key=False)
#                 buff[attr] = []
#                 gc.collect()
#             buff = {attr: [] for attr in attr_db.keys()}
#             return buff
#
#         p_bar = tqdm(desc=update_desc(), total=self.size())
#         if n_process == 1:
#             for i, iter_item in enumerate(reader_wikidata_dump(json_dump)):
#                 wd_respond = parse_json_dump(iter_item)
#                 # with closing(Pool(n_process)) as pool:
#                 #     for i, wd_respond in enumerate(
#                 #         pool.imap_unordered(parse_json_dump, iter_items)
#                 #     ):
#                 # if count > 10000:
#                 #     break
#                 if i and i % step == 0:
#                     p_bar.set_description(desc=update_desc())
#                     p_bar.update(step)
#                 if not wd_respond:
#                     continue
#                 wd_id, wd_obj = wd_respond
#                 lid = self.get_lid(wd_id)
#                 if lid is None:
#                     continue
#
#                 if wd_obj.get("claims") and wd_obj["claims"].get("wikibase-entityid"):
#                     if wd_obj["claims"]["wikibase-entityid"].get("P31"):
#                         instance_ofs = {
#                             i["value"]
#                             for i in wd_obj["claims"]["wikibase-entityid"]["P31"]
#                         }
#                         if cf.WIKIDATA_IDENTIFIERS.intersection(instance_ofs):
#                             continue
#                     if wd_obj["claims"]["wikibase-entityid"].get("P279"):
#                         subclass_ofs = {
#                             i["value"]
#                             for i in wd_obj["claims"]["wikibase-entityid"]["P279"]
#                         }
#                         if cf.WIKIDATA_IDENTIFIERS.intersection(subclass_ofs):
#                             continue
#                 count += 1
#
#                 for attr, value in wd_obj.items():
#                     if not value:
#                         continue
#
#                     if attr == "claims":
#                         encode_attr = {}
#                         for c_type, c_statements in value.items():
#                             encode_c_type = {}
#                             for c_prop, c_values in c_statements.items():
#                                 encode_c_prop = self.get_lid(c_prop, c_prop)
#                                 encode_c_values = []
#                                 for c_value in c_values:
#                                     decode_c_value = c_value["value"]
#                                     if c_type == "wikibase-entityid":
#                                         decode_c_value = self.get_lid(
#                                             decode_c_value, decode_c_value
#                                         )
#                                     elif c_type == "quantity":
#                                         if decode_c_value[1] != "1":
#                                             decode_c_value = (
#                                                 decode_c_value[0],
#                                                 self.get_lid(
#                                                     decode_c_value[1], decode_c_value[1]
#                                                 ),
#                                             )
#                                         else:
#                                             decode_c_value = (decode_c_value[0], -1)
#                                     encode_c_values.append(decode_c_value)
#
#                                 encode_c_type[encode_c_prop] = encode_c_values
#
#                             encode_attr[c_type] = encode_c_type
#                         value = encode_attr
#                         if value.get("wikibase-entityid"):
#                             for prop_lid, values_ent in value[
#                                 "wikibase-entityid"
#                             ].items():
#                                 if isinstance(prop_lid, int) and not any(
#                                     v for v in values_ent if not isinstance(v, int)
#                                 ):
#                                     key_ent = db_core.serialize_key(f"{lid}|{prop_lid}")
#                                     value_ent = db_core.serialize_value(
#                                         values_ent, bytes_value=ToBytesType.INT_NUMPY
#                                     )
#                                     buff_size += len(key_ent) + len(value_ent)
#                                     buff["claims_ent"].append((key_ent, value_ent))
#                             del value["wikibase-entityid"]
#
#                     if attr == "label":
#                         compress_value = False
#                     else:
#                         compress_value = True
#
#                     value = db_core.serialize_value(
#                         value, compress_value=compress_value
#                     )
#                     buff_size += len(value)
#                     buff[attr].append([lid, value])
#
#                 # Save buffer data
#                 if buff_size > cf.LMDB_BUFF_BYTES_SIZE:
#                     p_bar.set_description(desc=update_desc())
#                     buff = save_buff(buff)
#                     buff_size = 0
#
#             if buff_size:
#                 p_bar.set_description(desc=update_desc())
#                 save_buff(buff)
#                 buff_size = 0
