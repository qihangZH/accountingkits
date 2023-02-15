import pandas as pd
import requests


def api_usps_crosswalk_df(digital_token: str,
                          type_id: int,
                          query_id,
                          year: int,
                          quarter: int
                          ):
    """
    More information see https://www.huduser.gov/portal/dataset/uspszip-api.html

    :param digital_token: register to upper url for special token
    :param type_id:
    :param query_id:
    :param year:
    :param quarter:1,2,3,4
    :return: dataframe
    """
    if not (quarter in [1, 2, 3, 4]):
        raise ValueError('quarter should in 1,2,3,4; and it should be int')
    response_map_dict = {
        400: 'An invalid value was specified for one of the query parameters in the request URI.',
        401: 'Authentication failure',
        403: 'Not allowed to access this dataset API because you have not registered for it.',
        404: "No data found using '(value you entered)'",
        405: "Unsupported method, only GET is supported",
        406: "Unsupported Accept Header value, must be application/json",
        500: "Internal server error occurred"
    }

    url = f"https://www.huduser.gov/hudapi/public/usps?type={type_id}&query={query_id}&year={year}&quarter={quarter}"
    token = digital_token
    headers = {"Authorization": "Bearer {0}".format(token)}

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise ValueError(f"{response.status_code} Failure:{response_map_dict[response.status_code]}")

    else:
        temp_df = pd.DataFrame(response.json()["data"]["results"])

    return temp_df


def api_google_search_url_df(query_listarrser, target_urls_nums: int, method,
                                 is_auto_extend_stop: bool, request_deny_wait_time, sleep_func=None,
                                 **kwargs):
    pass