import tqdm
import time
import googlesearch
import fake_useragent
import warnings
import numpy as np
import pandas as pd

"""UNSTABLE, Have Not Passed the Test"""


def crawler_google_search_url_df(query_listarrser, target_urls_nums: int, method,
                                 is_auto_extend_stop: bool, request_deny_wait_time, sleep_func=None,
                                 **kwargs):
    """
    Use Google search to search the result of list of URLS, actually is a kind of crawler
    return url results

    :param query_listarrser: target search array-like
    :param target_urls_nums: the search result num propose to get
    :param method: 'thirdparty_loop',...
    :param sleep_func: AN NO ARGUMENT functions to sleep and be automatically 'pause' in googlesearch.search()
    :param is_auto_extend_stop: automatically add the 'stop' of googlesearch.search() for full results, or NA repeat
    :param request_deny_wait_time: how much secs to wait when request deny and try again
    :param kwargs: other arguments of googlesearch.search(),https://python-googlesearch.readthedocs.io/en/latest/
    """
    warnings.warn('AlphaVersion->This function is unstable to use for banned IP in test!', DeprecationWarning)

    if ('pause' in kwargs) and (not (sleep_func is None)):
        raise ValueError('Do not make pause/sleepfunc input together')

    kwargs['user_agent'] = kwargs['user_agent'] if 'user_agent' in kwargs else fake_useragent.UserAgent(
        browsers=["chrome", "edge",
                  "internet explorer", "firefox"]
    ).random

    query_ser = pd.Series(query_listarrser)

    if method == 'python_loop':

        agg_results_google_url_list = []

        for search_item in tqdm.tqdm(query_ser.to_numpy()):

            try:
                kwargs['pause'] = sleep_func()
            except:
                pass

            temp_search_list = []

            search_stop = kwargs['stop'] if 'stop' in kwargs else target_urls_nums

            if is_auto_extend_stop:
                while not (len(temp_search_list) >= target_urls_nums):
                    try:
                        temp_search_list = [search_result for search_result in googlesearch.search(search_item,
                                                                                                   **kwargs)]
                        search_stop += max(int(target_urls_nums / 2), 5)

                        temp_search_list = temp_search_list[:target_urls_nums] \
                            if len(temp_search_list) >= target_urls_nums \
                            else temp_search_list
                    except:
                        print(f'\nRequest denied, sleep for {request_deny_wait_time} seconds')
                        time.sleep(request_deny_wait_time)

            else:
                try:
                    temp_search_list = [search_result for search_result in googlesearch.search(search_item,
                                                                                               **kwargs)]

                    temp_search_list = temp_search_list[:target_urls_nums] \
                        if len(temp_search_list) >= target_urls_nums \
                        else np.concatenate(
                        [temp_search_list,
                         np.repeat(
                             '', target_urls_nums - len(temp_search_list)
                         )]
                    )
                except:
                    print(f'\nRequest denied, sleep for {request_deny_wait_time} seconds')
                    time.sleep(request_deny_wait_time)

            # only choose first item
            agg_results_google_url_list.append(
                np.array(temp_search_list[:target_urls_nums])[np.newaxis, :]
            )

        search_result_df = pd.DataFrame(np.concatenate(agg_results_google_url_list, axis=0),
                                        columns=[f'searchurl_{str(i)}' for i in range(target_urls_nums)])
        result_df = pd.concat([query_ser, search_result_df], axis=1)
    else:
        raise ValueError('Wrong input method and no results')

    return result_df
