import os.path
from typing import List, Dict

import requests
from tqdm.auto import tqdm

from config import config as cf
from mtab_server.utils import io_worker as iw
import subprocess


def download_file(download_url: str, download_dir: str, replace: bool = False):
    file_name = download_url.split("/")[-1]
    downloaded_file = f"{download_dir}/{file_name}"

    if os.path.exists(downloaded_file):
        if not replace:
            return downloaded_file
        else:
            iw.delete_file(downloaded_file)

    iw.create_dir(downloaded_file)
    r = requests.get(download_url, stream=True)
    if r.status_code != 200:
        return None
    p_bar = tqdm(
        total=int(r.headers.get("content-length", 0)),
        unit="B",
        unit_scale=True,
        desc=file_name,
    )
    with open(f"{download_dir}/{file_name}", "wb") as f:
        for data in r.iter_content(10240):
            p_bar.update(len(data))
            f.write(data)
    p_bar.close()
    return downloaded_file


def download_files(download_urls: List[str], download_dir: str, replace: bool = False):
    report = {}
    for download_url in download_urls:
        file_dir = download_file(download_url, download_dir, replace)
        if file_dir:
            report[download_url] = file_dir
    return report


def print_download_report(download_urls: List[str], report: Dict[str, str]):
    if not download_urls or not report:
        return
    for download_url in download_urls:
        file_dir = report.get(download_url)
        if file_dir:
            iw.print_status(f"Downloaded: {file_dir}")
        else:
            iw.print_status(f"Error     : {download_url}")


def download_dump_wikidata_json(
    json_ver: str,
    sql_ver: str,
    download_dir: str,
    get_bz2: bool = True,
    print_status: bool = False,
):
    sql_prefix = "https://dumps.wikimedia.org/wikidatawiki"
    json_prefix = "https://dumps.wikimedia.org/wikidatawiki/entities/"
    urls = [
        f"{sql_prefix}/{sql_ver}/wikidatawiki-{sql_ver}-page.sql.gz",
        f"{sql_prefix}/{sql_ver}/wikidatawiki-{sql_ver}-redirect.sql.gz",
    ]

    if get_bz2:
        urls.append(f"{json_prefix}/{json_ver}/wikidata-{json_ver}-all.json.bz2")
    else:
        urls.append(f"{json_prefix}/{json_ver}/wikidata-{json_ver}-all.json.gz")

    report = download_files(urls, download_dir)
    if print_status:
        print_download_report(urls, report)


def download_dump_wikipedia_html(
    ver: str, download_dir: str, langs: List[str], print_status: bool = False,
):
    html_prefix = "https://dumps.wikimedia.org/other/enterprise_html/runs/"
    urls = [
        f"{html_prefix}/{ver}/{lang}wiki-NS0-{ver}-ENTERPRISE-HTML.json.tar.gz"
        for lang in langs
    ]
    report = download_files(urls, download_dir)
    if print_status:
        print_download_report(urls, report)


def download_dump_wikipedia(
    ver: str, download_dir: str, langs: List[str], print_status: bool = False,
):
    urls = []
    prefix = "https://dumps.wikimedia.org"
    for lang in langs:
        urls.extend(
            [
                f"{prefix}/{lang}wiki/{ver}/{lang}wiki-{ver}-pages-articles.xml.bz2",
                f"{prefix}/{lang}wiki/{ver}/{lang}wiki-{ver}-page.sql.gz",
                f"{prefix}/{lang}wiki/{ver}/{lang}wiki-{ver}-page_props.sql.gz",
                f"{prefix}/{lang}wiki/{ver}/{lang}wiki-{ver}-redirect.sql.gz",
            ]
        )
    report = download_files(urls, download_dir)
    if print_status:
        print_download_report(urls, report)


def download_dump_dbpedia(download_dir):
    iw.create_dir(download_dir)

    subprocess.run(
        ["sh", f"{cf.DIR_ROOT}/mtab_server/utils/dbpedia_download.sh", download_dir],
        stdout=subprocess.DEVNULL,
    )
