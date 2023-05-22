import datetime
import re
import copy
import tldextract
import numpy as np
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from io import BytesIO
import multiprocessing.pool
import functools
import operator
from accountingkits import _BasicTools

"""
Author: Romain Boulland, Thomas Bourveau, Matthias Breuer
Date: Jun 24, 2021
Code: Generate bag of words for a time-series of company websites

* Please customize the config file before launching the program *

* Download the following nltk corpus before running the program *
nltk.download('stopwords')
nltk.download('punkt')
"""

"""
modified from origin files

Author: Qihang Zhang in 2023/02/03,National University of Singapore,
NUS Business School, Department of Accounting
"""


def wayback_url_parse_tuple(url):
    """
    The function wayback_urlparse() parses the timestamp and original URL for URLs seen during scraping
    """
    # Obtain timestamp and original URL for an archived URL
    if re.search(r'https://web.archive.org/web/[0-9]{14}/', url):
        timestamp = re.search('[0-9]{14}', url)[0]
        original = re.sub(r'https://web.archive.org/web/[0-9]{14}/', '', url)
        return datetime.datetime.strptime(timestamp, '%Y%m%d%H%M%S'), original
    else:
        return None, None


def wayback_url_query_df(host, match_type='exact', collapse_time=10, **kwargs):
    """
    The function wayback_query() interacts with the Wayback Machine API
     to retrieve all matched results for a given input URL.
    The official documentation of the API is here:
    https://github.com/internetarchive/wayback/tree/master/wayback-cdx-server
    """
    # Wayback API query
    q = 'http://web.archive.org/cdx/search/cdx?url={}&matchType={}&collapse=timestamp:{}&output=json' \
        .format(host, match_type, collapse_time)
    r = requests.get(q, **kwargs)
    # Convert json format result into a dataframe
    try:
        df = pd.read_json(r.content)
    except Exception as e:
        print(e)
        if not (r.content is None):
            df = pd.read_json(BytesIO(r.content))
        else:
            return None
    # If no match
    if df.shape[0] == 0:
        print('Wayback does not archive URLs under {}.'.format(host))
        return None
    # If matches returned
    else:
        # First row is column index
        df.columns = df.iloc[0]
        df = df.drop(df.index[0]).reset_index(drop=True)
        print('{} matched URLs found under {}.'.format(df.shape[0], host))
        # Search results cleaning: timestamp\
        # Drop observations with invalid timestamps
        df['datetime'] = pd.to_datetime(df['timestamp'], format='%Y%m%d%H%M%S', errors='coerce')
        if df['datetime'].isnull().any():
            df = df[df['datetime'].notnull()]
            print('{} matches left after dropping invalid timestamps.'.format(df.shape[0]))
        # Search results cleaning: urlkey
        # Ensure uniqueness of urlkey, keep the shortest if not
        if df['urlkey'].nunique() > 1:
            df = df[df['urlkey'] == sorted(df['urlkey'].to_numpy(), key=len)[0]]
            print('Urlkey not unique, keep the shortest, {} matches left.'.format(df.shape[0]))
        # Complete url for archived websites (as stored on archive.org)
        df['url'] = ['https://web.archive.org/web' + '/' + t + '/' + o for t, o in
                     zip(df['timestamp'].to_numpy(), df['original'].to_numpy())]
        df.sort_values(['urlkey', 'datetime'], inplace=True)
        # Output a txt if specified
        return df


@_BasicTools.DecoratorT.timer_wrapper
def wayback_url_suburls_search_df(archive_url, processes, recursive_times: int = 1, time_range=(None, None), **kwargs):
    def _lambda_search_suburls(url):
        try:
            response = requests.get(url, **kwargs)
            html_content = response.text

            soup = BeautifulSoup(html_content, "html.parser")

            anchor_tags = soup.find_all("a")

            sub_urls = []
            for anchor_tag in anchor_tags:
                href = anchor_tag.get("href")
                absolute_url = urljoin(url, href)
                sub_urls.append(absolute_url)
        except Exception as e:
            print(e)
            sub_urls = []

        return sub_urls

    url_ext = tldextract.extract(wayback_url_parse_tuple(archive_url)[1])
    url_domain = url_ext.domain + '.' + url_ext.suffix

    url_list = [archive_url]
    last_url_list = []

    for recurt in range(recursive_times):

        search_url_list = copy.deepcopy(
            list(set(url_list).symmetric_difference(set(last_url_list)))
        )
        # after find the search url list, change the last url list to new one.
        last_url_list = copy.deepcopy(url_list)

        if processes > 1:

            with multiprocessing.pool.ThreadPool(
                    processes=processes
            ) as pool:
                results = pool.map_async(_lambda_search_suburls, search_url_list,
                                         )
                current_results = functools.reduce(operator.add, results.get())

            url_list = url_list + current_results  # for safer use

        else:
            for surl in search_url_list:
                url_list = url_list + _lambda_search_suburls(surl)  # for safer use

        # make result to a set
        url_list = pd.Series(url_list).dropna().drop_duplicates().to_list()

        # remove the irrelavite urls
        url_list = pd.Series(url_list)[pd.Series(url_list).apply(
            lambda x: False if wayback_url_parse_tuple(x)[1] is None else
            url_domain in wayback_url_parse_tuple(x)[1]
        )].to_list()

        url_datetime_arr = pd.Series(url_list).apply(
            lambda x: wayback_url_parse_tuple(x)[0]
        ).to_numpy()

        if not ((time_range[0] is None) or (time_range[1] is None)):
            url_list = np.array(url_list)[
                (url_datetime_arr >= pd.to_datetime(time_range[0])) &
                (url_datetime_arr <= pd.to_datetime(time_range[1]))
                ].tolist()

    return list(set(url_list))
