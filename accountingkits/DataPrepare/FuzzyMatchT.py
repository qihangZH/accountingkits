import os
import typing
import tqdm
import numpy as np
import pandas as pd
import rapidfuzz
import signal
import pathos
import warnings
import math
import time


# --------------------------------------------------------------------------------------------
# functions(in-build)
# --------------------------------------------------------------------------------------------

def _duplicated_na_warning(text):
    """
    This warning message is tell user that Na/Null are all removed
    """
    warnings.warn(
        f"\n{text}: Duplicates or Null/Na are detected and automatically removed\n",
        UserWarning
    )


def _preprocess_input_query_choice_tuplelist(querying_listarr, choice_list):
    """
    reserve function...
    To make sure the query/choices list are input correctly

    There are two steps: 1.make sure no duplicates/NA 2.astype to string

    """

    # In new function, we add copy to make sure they are safe
    if np.any(pd.Series(querying_listarr).duplicated()) or np.any(pd.Series(querying_listarr).isna()):
        querying_listarr = pd.Series(querying_listarr).copy(
        ).dropna().drop_duplicates().to_list()

    if np.any(pd.Series(choice_list).duplicated()) or np.any(pd.Series(choice_list).isna()):
        choice_list = pd.Series(choice_list).copy(
        ).dropna().drop_duplicates().to_list()

    # In new function, we add copy to make sure they are safe
    """CONSTANTS CAN NOT BE CHANGED"""
    str_querying_list = pd.Series(querying_listarr).copy().astype(str).tolist()
    str_choice_list = pd.Series(choice_list).copy().astype(str).tolist()

    return str_querying_list, str_choice_list


def _build_fuzzymatch_query_choice_match_panel(query_l, choices_l, scorer, processes):
    """
    from query and choice list(No Na, must be query-list and choice-list)
    then return a panel with origin_query, match_list, and match_score
    """
    matchscores_total_mat = rapidfuzz.process.cdist(queries=query_l,
                                                    choices=choices_l,
                                                    scorer=scorer,
                                                    workers=processes)
    temp_df = pd.DataFrame(matchscores_total_mat,
                           index=query_l,
                           columns=choices_l
                           )
    temp_df.index.name = 'origin_query'
    temp_df.columns.name = 'match_list'

    stacked_temp_df = pd.DataFrame(temp_df.stack(), columns=[
        'match_score']).reset_index()

    return stacked_temp_df


def _chunksize_fuzzymatch_worker(worker_func, chunksize, query_list, choice_list, chunksize_taskbar: bool = True):
    """
    the worker function which input the cleaned query_l/choice_l and run the worker function in chunksize, save memory
    :return: chunk size using, should likely to result of worker_func(query_l, choice_l)
    """

    match_rst_df_slice_list = []

    task_iterator = tqdm.tqdm(range(0, len(query_list), chunksize)) \
        if chunksize_taskbar else range(0, len(query_list), chunksize)

    for s in task_iterator:
        slice_query_list = query_list[s:s + chunksize]
        match_rst_df_slice_list.append(
            worker_func(query_l=slice_query_list, choices_l=choice_list)
        )

    match_result_df = pd.concat(
        match_rst_df_slice_list, axis=0).reset_index(drop=True)

    return match_result_df


# --------------------------------------------------------------------------------------------
# functional-function, group using.
# --------------------------------------------------------------------------------------------


def group_fuzzymatch_df(list_fuzzymatch_func, query_group_df: pd.DataFrame, choices_group_df: pd.DataFrame,
                        groups: typing.Union[list, str], query_col, choice_col,
                        loop_method: typing.Literal[
                            'loop_in_parallel', 'parallel_in_loop'] = 'loop_in_parallel',
                        **kwargs):
    """
    the functional-function to input the func and make specialize outcomes.
    You should know that chunksize_taskbar/drop_na_null_warn are auto set False, not as their default in list_fuzzy...

    :param list_fuzzymatch_func: the worker function of list-fuzzymatch series
    :param query_group_df: the dataframe contains the info of groups columns and the target query col
    :param choices_group_df:  the dataframe contains the info of groups columns and the target choices col
    :param groups: the group column list
    :param query_col: the query col name
    :param choice_col: the choices col name
    :param loop_method: 'loop_in_parallel', 'parallel_in_loop', how to loop the task, default loop_in_parallel
    :param kwargs: MOST IMPORTANT, Except <querying_listarr, choice_list> are input, for each loop, other arguments
        should be input by yourself, if not then errors are unknown. For example, list_fuzzymatch_max_df
        <querying_listarr, choice_list> have be input,
        But scorer, processes=-1, chunksize=None, chunksize_taskbar: bool = True should be input by yourself.

    """
    # Kwargs and special basic arguments preprocess-----------------------------------------------

    # set each subprocess to 1 core using, if loop_in_parallel is set.
    # only return the processes with int bigger then 0
    if 'processes' in kwargs:
        processes = kwargs['processes'] if kwargs['processes'] > 0 else os.cpu_count(
        )
    else:
        processes = os.cpu_count()

    if loop_method == 'loop_in_parallel':
        kwargs['processes'] = 1

    # if No special input, then chunksize_taskbar should Be False for easy use:
    kwargs['chunksize_taskbar'] = kwargs['chunksize_taskbar'] if 'chunksize_taskbar' in kwargs else False
    kwargs['drop_na_null_warn'] = kwargs['drop_na_null_warn'] if 'drop_na_null_warn' in kwargs else False

    # first we have to check does the groups are all in dataframe
    groups_list = [groups] if isinstance(groups, str) else list(groups)
    groups_set = set(groups_list)

    if not (
            groups_set.issubset(set(query_group_df.columns.to_list())
                                )
            and
            groups_set.issubset(set(choices_group_df.columns.to_list())
                                )
    ):
        raise ValueError(
            'query_group_df/choices_group_df columns must contain groups_list')

    # Sub-functions which use kwargs-------------------------------------------------------------

    def _worker_loop_fmatch_df(_t_tuple_list):
        grp_match_rst_df_list = []
        for taskgrp, qgrpdf, cgrpdf in tqdm.tqdm(_t_tuple_list):
            tmp_match_df = list_fuzzymatch_func(
                querying_listarr=qgrpdf[query_col].to_list(),
                choice_list=cgrpdf[choice_col].to_list(),
                **kwargs
            ).copy()

            # we should make sure group is a list:
            taskgrp_list = [taskgrp] if isinstance(
                taskgrp, str) else list(taskgrp)
            # add back groups: we know position and have add back
            for pos in range(len(groups_list)):
                tmp_match_df[groups_list[pos]] = taskgrp_list[pos]

            grp_match_rst_df_list.append(
                tmp_match_df
            )

        return pd.concat(grp_match_rst_df_list, axis=0).reset_index(drop=True)

    # Main part-Preprocessing ------------------------------------------------------------------

    preprocess_timestart = time.time()

    # first we pick the data from choices groups for further using
    choices_grps_list = []
    choices_grpdfs_list = []

    for cgrps, cgrpsdfs in choices_group_df.groupby(by=groups_list):
        choices_grps_list.append(cgrps)
        choices_grpdfs_list.append(cgrpsdfs)

    # secondly we have the query groups, and directly match them to the task tuple:
    """core task list"""
    task_tuple_list = []

    for qgrps, qgrpsdfs in query_group_df.groupby(by=groups_list):

        # we have a var to save task's choice group index and pick it.
        cgrps_index = None
        # we have to check if the query group is in choices groups?
        for index, cgrps in enumerate(choices_grps_list):
            if qgrps == cgrps:
                cgrps_index = index
                break
        # if not find then break, however.
        if not cgrps_index:
            continue
        else:
            task_tuple_list.append(
                (qgrps, qgrpsdfs, choices_grpdfs_list[cgrps_index]))

    print(
        f'preprocessing(grouping the dataframes) cost {time.time() - preprocess_timestart}')

    if loop_method == 'loop_in_parallel':

        _chunk = math.ceil(len(task_tuple_list) / processes)

        task_tuple_chunked_list = [
            task_tuple_list[s: s + _chunk]
            for s in range(0, len(task_tuple_list), _chunk)
        ]

        mp_task_rst_df_list = []
        with pathos.multiprocessing.Pool(processes=processes,
                                         initializer=lambda: signal.signal(
                                             signal.SIGINT, signal.SIG_IGN)
                                         ) as pool:
            for results in pool.imap_unordered(_worker_loop_fmatch_df, iterable=task_tuple_chunked_list):
                mp_task_rst_df_list.append(results)

        return pd.concat(mp_task_rst_df_list, axis=0).reset_index(drop=True)

    elif loop_method == 'parallel_in_loop':
        return _worker_loop_fmatch_df(_t_tuple_list=task_tuple_list)


# --------------------------------------------------------------------------------------------
# list_fuzzymatch..._df function series, main functions
# --------------------------------------------------------------------------------------------

def list_fuzzymatch_max_df(querying_listarr, choice_list, scorer, processes=-1, chunksize=None,
                           chunksize_taskbar: bool = True, drop_na_null_warn: bool = True):
    """
    lisklikearray->fuzzymatch with a LIST. scorer I prefer rapidfuzz.fuzz.ratio(Normalize levenshtein)
    Difference from list_fuzzymatching_df: only have rapidfuzz_cdist, while it not only return the result
    of biggest chance, but also the any choices NUMS EQUAL to max one(largest score...)
    , while, it would not return any observs
    which not exceed the threshold.

    :param querying_listarr:~
    :param choice_list:~
    :param scorer: the scorer of fuzzymatch, it could be changed to different result,
            classic normalized_levenshtein be rapidfuzz.fuzz.ratio,however be useless in difflib method
    :param processes: the multiprocesses works number, default be all cores
    :param chunksize: default None, else with slice the query to save the memory.
    :param chunksize_taskbar: default True, if or not use taskbar of chunksize
    :param drop_na_null_warn: Is or not tell the user that the NA/NULL are already drop(duplicated)
    :return: result dataframe, columns: origin_query, match_list, match_score
    """

    def _lambda_fuzzymatch_max(query_l, choices_l):
        """SuperFast Cpp Matrix method for matching result"""
        matchscores_total_mat = rapidfuzz.process.cdist(queries=query_l,
                                                        scorer=scorer,
                                                        choices=choices_l,
                                                        workers=processes)
        matchscores_position_arr = np.nanargmax(matchscores_total_mat, axis=1)
        # In after place match mean result from choice list
        final_stk_tp_df = pd.DataFrame(
            {
                'origin_query': np.array(query_l),
                'match_list': np.array(choices_l)[matchscores_position_arr],
                'match_score': matchscores_total_mat[np.arange(len(query_l)), matchscores_position_arr],
            }
        )

        return final_stk_tp_df

    # cleaned the data/ preprocess
    cleaned_querying_list, cleaned_choice_list = _preprocess_input_query_choice_tuplelist(
        querying_listarr, choice_list
    )

    if drop_na_null_warn:
        _duplicated_na_warning('query-Seq and choice-Seq')

    if chunksize:
        match_result_df = _chunksize_fuzzymatch_worker(
            worker_func=_lambda_fuzzymatch_max,
            chunksize=chunksize,
            query_list=cleaned_querying_list,
            choice_list=cleaned_choice_list,
            chunksize_taskbar=chunksize_taskbar
        )

    else:
        """if not chunksize is given"""
        match_result_df = _lambda_fuzzymatch_max(query_l=cleaned_querying_list,
                                                 choices_l=cleaned_choice_list)

    return match_result_df


def list_fuzzymatch_nlargests_df(querying_listarr, choice_list, scorer, max_nums: int, keep='first', processes=-1,
                                 chunksize=None, chunksize_taskbar: bool = True, drop_na_null_warn: bool = True):
    """
    lisklikearray->fuzzymatch with a LIST. scorer I prefer rapidfuzz.fuzz.ratio(Normalize levenshtein)
    Difference from list_fuzzymatching_df: only have rapidfuzz_cdist, while it not only return the result
    of biggest chance, but also the any choices NUMS EQUAL to max_nums(or less then if choices are not enough...)
    , while, it would not return any observs
    which not exceed the threshold.

    :param querying_listarr:~
    :param choice_list:~
    :param scorer: the scorer of fuzzymatch, it could be changed to different result,
            classic normalized_levenshtein be rapidfuzz.fuzz.ratio,however be useless in difflib method
    :param max_nums: the max numbers to choose from choices, per query
    :param keep: the param of https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.nlargest.html
    :param processes: the multiprocesses works number, default be all cores
    :param chunksize: default None, else with slice the query to save the memory.
    :param chunksize_taskbar: default True, if or not use taskbar of chunksize
    :param drop_na_null_warn: Is or not tell the user that the NA/NULL are already drop(duplicated)
    :return: result dataframe, columns: origin_query, match_list, match_score
    """

    def _lambda_fuzzymatch_nlargest(query_l, choices_l):
        """SuperFast Cpp Matrix method for matching result"""
        stacked_temp_df = _build_fuzzymatch_query_choice_match_panel(query_l=query_l,
                                                                     choices_l=choices_l,
                                                                     scorer=scorer,
                                                                     processes=processes
                                                                     )

        final_stk_tp_df = stacked_temp_df.groupby(by=['origin_query']).apply(
            lambda x: x.nlargest(n=max_nums, columns='match_score', keep=keep)
        ).reset_index(level=['origin_query'], drop=True)

        return final_stk_tp_df

    # cleaned the data/ preprocess
    cleaned_querying_list, cleaned_choice_list = _preprocess_input_query_choice_tuplelist(
        querying_listarr, choice_list
    )

    if drop_na_null_warn:
        _duplicated_na_warning('query-Seq and choice-Seq')

    if chunksize:
        match_result_df = _chunksize_fuzzymatch_worker(
            worker_func=_lambda_fuzzymatch_nlargest,
            chunksize=chunksize,
            query_list=cleaned_querying_list,
            choice_list=cleaned_choice_list,
            chunksize_taskbar=chunksize_taskbar
        )

    else:
        """if not chunksize is given"""
        match_result_df = _lambda_fuzzymatch_nlargest(query_l=cleaned_querying_list,
                                                      choices_l=cleaned_choice_list)

    return match_result_df


def list_fuzzymatch_threshold_df(querying_listarr, choice_list, scorer, threshold, processes=-1, chunksize=None,
                                 chunksize_taskbar: bool = True, drop_na_null_warn: bool = True
                                 ):
    """
    lisklikearray->fuzzymatch with a LIST. scorer I prefer rapidfuzz.fuzz.ratio(Normalize levenshtein)
    Difference from list_fuzzymatching_df: only have rapidfuzz_cdist, while it not only return the result
    of biggest chance, but also the any choices bigger then the threshold, while, it would not return any observs
    which not exceed the threshold.

    :param querying_listarr:~
    :param choice_list:~
    :param scorer: the scorer of fuzzymatch, it could be changed to different result,
            classic normalized_levenshtein be rapidfuzz.fuzz.ratio,however be useless in difflib method
    :param threshold: the threshold of keep the observation, should be number from 0~100
    :param processes: the multiprocesses works number, default be all cores
    :param chunksize: default None, else with slice the query to save the memory.
    :param chunksize_taskbar: default True, if or not use taskbar of chunksize
    :param drop_na_null_warn: Is or not tell the user that the NA/NULL are already drop(duplicated)
    :return: result dataframe, columns: origin_query, match_list, match_score
    """

    def _lambda_fuzzymatch_threshold(query_l, choices_l):
        """SuperFast Cpp Matrix method for matching result"""
        stacked_temp_df = _build_fuzzymatch_query_choice_match_panel(query_l=query_l,
                                                                     choices_l=choices_l,
                                                                     scorer=scorer,
                                                                     processes=processes
                                                                     )

        final_stk_tp_df = stacked_temp_df[stacked_temp_df['match_score'] > threshold]

        return final_stk_tp_df

    # cleaned the data/ preprocess
    cleaned_querying_list, cleaned_choice_list = _preprocess_input_query_choice_tuplelist(
        querying_listarr, choice_list
    )

    if drop_na_null_warn:
        _duplicated_na_warning('query-Seq and choice-Seq')

    if chunksize:
        match_result_df = _chunksize_fuzzymatch_worker(
            worker_func=_lambda_fuzzymatch_threshold,
            chunksize=chunksize,
            query_list=cleaned_querying_list,
            choice_list=cleaned_choice_list,
            chunksize_taskbar=chunksize_taskbar
        )

    else:
        """if not chunksize is given"""
        match_result_df = _lambda_fuzzymatch_threshold(query_l=cleaned_querying_list,
                                                       choices_l=cleaned_choice_list)

    return match_result_df
