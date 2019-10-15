import errno
import linecache
import mimetypes
import os
import signal
import sys
from datetime import datetime
from functools import wraps
from urllib.parse import urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from validator_collection import checkers

EN_KEY_WORDS = ["debt", "statistical", "statistics", "bulletin", "monthly", "report", "strategy"]


def timeout(seconds=10, error_message=os.strerror(errno.ETIME)):
    def decorator(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message)

        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return wraps(func)(wrapper)

    return decorator


def is_downloadable(url):
    """
    Does the url contain a downloadable resource
    """
    try:
        file_extensions = ["pdf", "docx", "doc", "xls", "xlsx" "csv"]
        content_type_list = ["pdf", "word", "excel", "officedocument"]
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return None
        headers = response.headers
        content_type = headers.get("Content-Type")

        for f in content_type:
            if f in content_type_list:
                return True

        filename = url.split("/")[-1]
        for e in file_extensions:
            if e in filename:
                return True

        if 'text' in content_type.lower():
            return False
        if 'html' in content_type.lower():
            return False
        
        return True
    except Exception:
        return False


def get_weblinks(fpath=None):
    """
    Gets website links and put them in a data frame
    :param fpath:
    :return:
    """

    df = pd.read_csv(fpath)
    df["Link"].fillna("No link yet", inplace=True)
    country_dict = {}

    for index, row in df.iterrows():
        country_code = row["Country Code"]
        country_name = row["DMF Country name"]
        url = row["Link"]
        country_dict[country_code] = {"targetUrl": url, "countryName": country_name}

    return country_dict


def get_links_from_target_sites(url=None):
    """
    Given the website, gets the links
    :param url:
    :return:
    """
    # TODO: detect webpage language and return it
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return None
        html = response.text
        bs = BeautifulSoup(html, "html.parser")
        urls = {}
        for a in bs.find_all('a', href=True):
            urls[a['href']] = a.getText()
        return urls
    except Exception:
        return None


def retrieve_filename_and_extension(response=None, url=None):
    file_extensions = ["pdf", "docx", "doc", "xls", "xlsx" "csv"]
    content_type_list = ["pdf", "word", "excel", "officedocument"]
    try:
        filename = url.split("/")[-1]
        for e in file_extensions:
            if e in filename:
                return filename
        
        content_type = response.headers['content-type']
        for c in content_type_list:
            if c in content_type.lower():
                extension = mimetypes.guess_extension(content_type)
                filename = url.split("/")[-1] + extension
                return filename
    except Exception:
        return None


def download_file(url=None, outfolder=None):
    """
    Download and save file if possible
    :param url:
    :param outfolder:
    :return:
    """
    try:
        file = requests.get(url, allow_redirects=True, timeout=10)
        if file.status_code != 200:
            return None
        filename = retrieve_filename_and_extension(response=file, url=url)
        fpath = os.path.join(outfolder, filename)
        open(fpath, 'wb').write(file.content)
        return filename
    except Exception:
        return None


def download_content(content_url=None, output_folder=None, base_url=None, visit_log=None):
    """
    Given a list of links extracted from seed list of websites
    download relevant content
    :param urls: a dict object with country details and urls
    :param key_words: key words to determine what content to download
    :return:
    """
    try:
        if checkers.is_url(content_url):
            if is_downloadable(content_url):
                return download_file(url=content_url, outfolder=output_folder)
            else:
                return scrape_docs_from_links(seed_url=content_url, output_dir=output_folder,
                                              root_url=base_url, seen_links=visit_log, relevant=True)
        else:
            parsed_uri = urlparse(base_url)
            root_url = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
            revised_link = root_url[:-1] + content_url
            return download_content(content_url=revised_link, output_folder=output_folder,
                                    base_url=revised_link, visit_log=visit_log)
    except Exception:
        exc_type, exc_obj, tb = sys.exc_info()
        f = tb.tb_frame
        lineno = tb.tb_lineno
        filename = f.f_code.co_filename
        linecache.checkcache(filename)
        line = linecache.getline(filename, lineno, f.f_globals)
        print('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))


def get_relevant_links(starter_url=None, has_key_words=True):
    links = get_links_from_target_sites(url=starter_url)
    if not links:
        return None

    relevant_urls = [starter_url]
    for l, d in links.items():
        for w in EN_KEY_WORDS:
            if w in l.lower():
                relevant_urls.append(l)
                break
            if d:
                if w in d.lower():
                    relevant_urls.append(l)
                    break
        for e in ["pdf", "docx", "doc", "xls", "xlsx" "csv"]:
            if e in l:
                relevant_urls.append(l)
                break
    return relevant_urls


def scrape_docs_from_links(seed_url=None, output_dir=None, root_url=None, seen_links=None, relevant=False):
    relevant_urls = set(get_relevant_links(starter_url=seed_url, has_key_words=relevant))
    if not relevant_urls:
        return None

    files_downloaded = []
    for u in relevant_urls:
        print("========================================")
        print(u)
        print("========================================")
        try:
            if u in seen_links:
                seen_links[u] = seen_links[u] + 1
            else:
                seen_links[u] = 1
            if seen_links[u] > 2:
                continue
            res = download_content(content_url=u, output_folder=output_dir, base_url=root_url, visit_log=seen_links)
            if res:
                files_downloaded.append({"downloadedFileName": res, "downloadUrl": u})
        except Exception as e:
            exc_type, exc_obj, tb = sys.exc_info()
            f = tb.tb_frame
            lineno = tb.tb_lineno
            filename = f.f_code.co_filename
            linecache.checkcache(filename)
            line = linecache.getline(filename, lineno, f.f_globals)
            print('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))
            pass

    return files_downloaded


def process_single_country(url=None, downloads_dir=None, country_code=None):
    """
    A helper function which calls the rest of the functions to download data
    :param downloads_dir:
    :return:
    """
    # ========================================
    # SET UP DIRECTORY TO PUT FILES
    # ========================================
    country_downloads_dir = os.path.join(downloads_dir, country_code)
    if not os.path.exists(country_downloads_dir):
        os.makedirs(country_downloads_dir)

    # =========================================
    # DOWNLOAD FILES WHERE AVAILABLE
    # =========================================
    links_visits = {url: 1}
    downloaded_files = scrape_docs_from_links(seed_url=url, output_dir=country_downloads_dir,
                                              root_url=url, seen_links=links_visits, relevant=True)
    return downloaded_files


def process_all_countries(metadata_outfile=None, country_web_links=None, downloads_dir=None, country_list=None):
    """
    Gets the download results and saves into CSV
    :param results:
    :return:
    """
    urls = get_weblinks(fpath=country_web_links)
    ts = datetime.now()
    time_format = "%m-%d-%Y %H:%M"
    ts_str = ts.strftime(time_format)

    df_data = []
    data_pt = {"dateProcessed": ts_str, "keyWords": EN_KEY_WORDS, "comment": None,
               "downloadRelevant": None, "status": None}
    
    for k, v in urls.items():
        if k not in country_list:
            continue
        try:
            print("Working on country : {}".format(k))
            print()
            res = process_single_country(url=v["targetUrl"], downloads_dir=downloads_dir, country_code=k)
            data_pt["countryName"] = v["countryName"]
            data_pt["countryCode"] = k
            data_pt["targetUrl"] = v["targetUrl"]
            if data_pt["targetUrl"] == "No link yet":
                data_pt["comment"] = 'Need to find MoF weblink'
                data_pt["status"] = "Not started"
                df_data.append(data_pt)

            if res:
                for item in res:
                    data_pt.update(item)
                    data_pt["status"] = "Complete"
                    df_data.append(data_pt)
            else:
                data_pt["comment"] = 'Something went wrong'
                data_pt["status"] = 'in progress'
                df_data.append(data_pt)
        except Exception as e:
            exc_type, exc_obj, tb = sys.exc_info()
            f = tb.tb_frame
            lineno = tb.tb_lineno
            filename = f.f_code.co_filename
            linecache.checkcache(filename)
            line = linecache.getline(filename, lineno, f.f_globals)
            print('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))
            print("Failed to complete work for this country")
            pass

    df = pd.DataFrame(df_data)
    cols = ["countryName", "countryCode", "targetUrl", "downloadUrl", "downloadedFileName",
            "dateProcessed", "keyWords", "downloadRelevant", "comment", 'status']
    df = df[cols]
    df.to_csv(metadata_outfile, index=False)


def main():
	# replace the paths below with your paths
    base_dir = os.path.abspath("/Users/dmatekenya/Google-Drive/teachingAndLearning/SIAP-oct-2019/python-for-data-science/")
    downloads_folder = os.path.join(base_dir, "fileDownloads")
    weblinks_csv = os.path.join(base_dir, "data", "webLinks.csv")
    meta_outfile = os.path.join(base_dir, "fileDownloads", "processingLogs.csv")
    countries = ["ETH", "BEN", "GMB", "GHA"]
    process_all_countries(metadata_outfile=meta_outfile, country_web_links=weblinks_csv,
                          downloads_dir=downloads_folder, country_list=countries)
    print("DONE")


if __name__ == '__main__':
    main()
