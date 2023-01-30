import pandas as pd
import requests
import typing


def usps_crosswalk_api_df(digital_token: str,
                          type_id: int,
                          query_id,
                          **kwargs
                          ):
    """
    More information see https://www.huduser.gov/portal/dataset/uspszip-api.html
    :param digital_token:
    :param type_id:
    :param query_id:
    :param kwargs: other arguments of huduser.gov
    :return:
    """
    temp_usps_api_args_str = ''
    for i in kwargs:
        temp_usps_api_args_str = temp_usps_api_args_str + f'&{i}={kwargs[i]}'

    response_map_dict = {
        400: 'An invalid value was specified for one of the query parameters in the request URI.',
        401: 'Authentication failure',
        403: 'Not allowed to access this dataset API because you have not registered for it.',
        404: "No data found using '(value you entered)'",
        405: "Unsupported method, only GET is supported",
        406: "Unsupported Accept Header value, must be application/json",
        500: "Internal server error occurred"
    }

    url = f"https://www.huduser.gov/hudapi/public/usps?type={type_id}&query={query_id}"+temp_usps_api_args_str

    token = digital_token

    headers = {"Authorization": "Bearer {0}".format(token)}

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise ValueError(f"{response.status_code} Failure:{response_map_dict[response.status_code]}")

    else:
        temp_df = pd.DataFrame(response.json()["data"]["results"])

    return temp_df
