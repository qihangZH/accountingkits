import json

import pandas as pd

"""
modified from origin JsonConverterT.py

Refactor: Qihang Zhang in 2023/02/03,National University of Singapore,
NUS Business School, Department of Accounting

"""


def wayback_scraper_response_json_to_df(PATH):
    """
    :param PATH: path of WaybackT.WayBack.wayback_scraper returned json
    should be res[...].json like files, However you may change the name.
    """

    if not PATH.endswith('.json'):
        raise ValueError('Input file name must be json')

    # read result json
    with open(PATH, "r") as file:
        data_dict = json.load(file)

    # set the containers
    q_url_list = []
    url_list = []
    c_url_list = []
    error_list = []
    text_list = []

    # one chronic should have one subset
    for c in data_dict.keys():
        # chronic data list
        chronic_data_list = data_dict[c]
        # there should be obs dicts in chronic data list
        for obsdict in chronic_data_list:
            q_url_list.append(c)
            url_list.append(obsdict['URL'])
            c_url_list.append(obsdict['cURL'])
            error_list.append(obsdict['error'])
            text_list.append(obsdict['text'])

    result_df = pd.DataFrame(
        {
            'q_url': q_url_list,
            'url': url_list,
            'c_url': c_url_list,
            'error': error_list,
            'text': text_list
        }
    ).copy()

    result_df['q_date'] = pd.to_datetime(
        result_df['q_url'].str.extract(
            r'^https://web\.archive\.org/web/(\d+)/.*$', expand=False
        ),
        format="%Y%m%d%H%M%S"
    )

    result_df['url_date'] = pd.to_datetime(
        result_df['url'].str.extract(
            r'^https://web\.archive\.org/web/(\d+)/.*$', expand=False
        ),
        format="%Y%m%d%H%M%S"
    )

    return result_df
