import multiprocessing.pool
import pandas as pd
import requests
import bs4
import re
from .. import _BasicTools


def test_float_proxy_working_bool(proxies, timeout=180):
    """test what ever request.get() proxies is working(float proxies)"""
    try:
        res1 = re.search(r'[.\d]+', requests.get('http://httpbin.org/ip',
                                                 proxies=proxies, timeout=timeout).text

                         ).group()
        res2 = re.search(r'[.\d]+', requests.get('http://httpbin.org/ip',
                                                 proxies=proxies, timeout=timeout).text).group()
        if res1 != res2:
            return True
        else:
            return False
    except Exception as e:
        print(e)
        return False


def request_html_url_text_str(url, **kwargs) -> str:
    try:
        response = requests.get(url, **kwargs)
        if response.status_code == 200:
            content_type = response.headers.get('Content-Type', '')
            if 'html' in content_type:
                parser = 'html.parser'
            elif 'xml' in content_type:
                parser = 'lxml-xml'
            else:
                parser = 'html.parser'  # Default to HTML parser

            soup = bs4.BeautifulSoup(response.content, parser)

            for script in soup(['script', 'style']):
                script.extract()

            text = soup.get_text()

            return text
        else:
            print(f"Error: Unable to fetch the content. Status code: {response.status_code}")
            return ''
    except Exception as e:
        print(e)
        return ''


@_BasicTools.DecoratorT.timer_wrapper
def mt_list_request_html_url_text_df(url_list, processes, **kwargs) -> pd.DataFrame:

    with multiprocessing.pool.ThreadPool(
            processes=processes,
    ) as pool:
        results = pool.map_async(
            lambda x: request_html_url_text_str(x, **kwargs),
            iterable=url_list
        )

        result_df = pd.DataFrame(
            {'url': url_list,
             'text': [i for i in results.get()]}
        )

    return result_df
