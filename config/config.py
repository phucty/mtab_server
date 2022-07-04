import csv
import re

import psutil

# Configuration
ENCODING = "utf-8"
VER_WD_SQL = "20220601"
VER_WD_TRUTHY = "20220521"
VER_WD_JSON = "20220606"

VER_WP = "20220601"

# Directories
DIR_ROOT = "/Users/phucnguyen/git/mtab_server"
DIR_CONFIG = f"{DIR_ROOT}/config"

DIR_DUMPS_WD = "/tmp/dumps/wikidata"
DIR_DUMPS_WP = "/tmp/dumps/wikipedia"
DIR_DUMPS_DP = "/tmp/dumps/dbpedia/latest"

DIR_DUMP_WD_JSON = f"{DIR_DUMPS_WD}/wikidata-{VER_WD_JSON}-all.json.bz2"
DIR_DUMP_WD_TRUTHY = f"{DIR_DUMPS_WD}/latest-truthy.nt.bz2"
DIR_DUMP_WD_PAGE = f"{DIR_DUMPS_WD}/wikidatawiki-{VER_WD_SQL}-page.sql.gz"
DIR_DUMP_WD_REDIRECT = f"{DIR_DUMPS_WD}/wikidatawiki-{VER_WD_SQL}-redirect.sql.gz"

DIR_DUMP_WP_EN = f"{DIR_DUMPS_WP}/enwiki-{VER_WP}-pages-articles.xml.bz2"
# Wikipedia SQL dump for ID mapping - Wikipedia - Wikidata
DIR_DUMP_WP_PAGE = f"{DIR_DUMPS_WP}/enwiki-{VER_WP}-page.sql.gz"
DIR_DUMP_WP_PROPS = f"{DIR_DUMPS_WP}/enwiki-{VER_WP}-page_props.sql.gz"
DIR_DUMP_WP_REDIRECT = f"{DIR_DUMPS_WP}/enwiki-{VER_WP}-redirect.sql.gz"

# DBpedia Data bus dump
DIR_DUMP_DP_WP = f"{DIR_DUMPS_DP}/wikipedia-links_lang=en.ttl.bz2"

DIR_DUMP_DP_WD = f"{DIR_DUMPS_DP}/ontology--DEV_type=parsed_sorted.nt"
DIR_DUMP_DP_REDIRECT = f"{DIR_DUMPS_DP}/redirects_lang=en.ttl.bz2"

DIR_DUMP_DP_LABELS = f"{DIR_DUMPS_DP}/labels_lang=en.ttl.bz2"
DIR_DUMP_DP_DESC = f"{DIR_DUMPS_DP}/short-abstracts_lang=en.ttl.bz2"
DIR_DUMP_DP_TYPES_SPECIFIC = f"{DIR_DUMPS_DP}/instance-types_lang=en_specific.ttl.bz2"
DIR_DUMP_DP_TYPES_TRANSITIVE = (
    f"{DIR_DUMPS_DP}/instance-types_lang=en_transitive.ttl.bz2"
)
DIR_DUMP_DP_INFOBOX = f"{DIR_DUMPS_DP}/infobox-properties_lang=en.ttl.bz2"
DIR_DUMP_DP_OBJECTS = f"{DIR_DUMPS_DP}/mappingbased-objects_lang=en.ttl.bz2"
DIR_DUMP_DP_LITERALS = f"{DIR_DUMPS_DP}/mappingbased-literals_lang=en.ttl.bz2"
DIR_DUMP_DP_DISAMBIGUATION = f"{DIR_DUMPS_DP}/disambiguations_lang=en.ttl.bz2"


DIR_MODELS = f"{DIR_ROOT}/data/models"
DIR_WIKIDB = f"{DIR_MODELS}/wikidb"


class ATTR_OPTS:
    AND = "AND"
    OR = "OR"
    NOT = "NOT"


WD = "http://www.wikidata.org/entity/"
WDT = "http://www.wikidata.org/prop/direct/"


def read_tsv_file_first_col(file_name):
    with open(file_name, encoding=ENCODING) as f:
        first_col = [l[0].rstrip() for l in csv.reader(f, delimiter="\t")]
    return first_col


WIKIDATA_IDENTIFIERS = read_tsv_file_first_col(f"{DIR_CONFIG}/WD_IDENTIFIERS.tsv")
# WP_IGNORED_NS = read_tsv_file_first_col(f"{DIR_CONFIG}/WP_IGNORED_NS.tsv")
# 105 languages as mGENRE De Cao et al.
# LANGS_105 = read_tsv_file_first_col(f"{DIR_CONFIG}/LANGS_105.tsv")
# 322 languages of Wikipedia
LANGS_322 = read_tsv_file_first_col(f"{DIR_CONFIG}/LANGS_322.tsv")
# LANGS_SELECTED = read_tsv_file_first_col(f"{DIR_CONFIG}/LANGS_SELECTED.tsv")
LANGS = LANGS_322

WP_NAMESPACE_RE = re.compile(r"^{(.*?)}")
WP_DISAMBIGUATE_REGEXP = re.compile(
    r"{{\s*(disambiguation|disambig|disamb|dab|geodis)\s*(\||})", re.IGNORECASE
)

HTML_HEADERS = read_tsv_file_first_col(f"{DIR_CONFIG}/TAGS_HTML_HEADERS.tsv")
SPACY_NER_TAGS = read_tsv_file_first_col(f"{DIR_CONFIG}/NER_TAGS_SPACY.tsv")
LANGS_SPACY = read_tsv_file_first_col(f"{DIR_CONFIG}/LANGS_SPACY.tsv")


SIZE_1MB = 1_048_576
SIZE_512MB = 536_870_912
SIZE_1GB = 1_073_741_824

LMDB_MAX_KEY = 511
LMDB_MAP_SIZE = 10_737_418_240  # 10GB
# Using Ram as buffer
LMDB_BUFF_BYTES_SIZE = psutil.virtual_memory().total // 10
if LMDB_BUFF_BYTES_SIZE > SIZE_1GB:
    LMDB_BUFF_BYTES_SIZE = SIZE_1GB


BUFF_LIMIT = SIZE_1GB
