import tqdm
import numpy as np
import pandas as pd
import rapidfuzz


def _preprocess_input_query_choice_tuplelist(querying_listarr, choice_list):
    """
    reserve function...
    To make sure the query/choices list are input correctly

    There are two steps: 1.make sure no duplicates/NA 2.astype to string

    """

    # In new function, we add copy to make sure they are safe
    if np.any(pd.Series(querying_listarr).duplicated()) or np.any(pd.Series(querying_listarr).isna()):
        print(
            "\nDuplicates or Null/Na in querying are detected and automatically removed\n"
        )

        querying_listarr = pd.Series(querying_listarr).copy().dropna().drop_duplicates().to_list()

    if np.any(pd.Series(choice_list).duplicated()) or np.any(pd.Series(choice_list).isna()):
        print(
            "\nDuplicates or Null/Na in choices are detected and automatically removed\n"
        )
        choice_list = pd.Series(choice_list).copy().dropna().drop_duplicates().to_list()

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

    stacked_temp_df = pd.DataFrame(temp_df.stack(), columns=['match_score']).reset_index()

    return stacked_temp_df


def _chunksize_fuzzymatch_worker(worker_func, chunksize, query_list, choice_list):
    """
    the worker function which input the cleaned query_l/choice_l and run the worker function in chunksize, save memory
    :return: chunk size using, should likely to result of worker_func(query_l, choice_l)
    """

    match_rst_df_slice_list = []

    for s in tqdm.tqdm(range(0, len(query_list), chunksize)):
        slice_query_list = query_list[s:s + chunksize]
        match_rst_df_slice_list.append(
            worker_func(query_l=slice_query_list, choices_l=choice_list)
        )

    match_result_df = pd.concat(match_rst_df_slice_list, axis=0).reset_index(drop=True)

    return match_result_df


def list_fuzzymatch_max_df(querying_listarr, choice_list, scorer, processes=-1, chunksize=None):
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

    if chunksize:
        match_result_df = _chunksize_fuzzymatch_worker(
            worker_func=_lambda_fuzzymatch_max,
            chunksize=chunksize,
            query_list=cleaned_querying_list,
            choice_list=cleaned_choice_list
        )

    else:
        """if not chunksize is given"""
        match_result_df = _lambda_fuzzymatch_max(query_l=cleaned_querying_list,
                                                 choices_l=cleaned_choice_list)

    return match_result_df


def list_fuzzymatch_nlargests_df(querying_listarr, choice_list, scorer, max_nums: int, keep='first', processes=-1,
                                 chunksize=None):
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

    if chunksize:
        match_result_df = _chunksize_fuzzymatch_worker(
            worker_func=_lambda_fuzzymatch_nlargest,
            chunksize=chunksize,
            query_list=cleaned_querying_list,
            choice_list=cleaned_choice_list
        )

    else:
        """if not chunksize is given"""
        match_result_df = _lambda_fuzzymatch_nlargest(query_l=cleaned_querying_list,
                                                      choices_l=cleaned_choice_list)

    return match_result_df


def list_fuzzymatch_threshold_df(querying_listarr, choice_list, scorer, threshold, processes=-1, chunksize=None):
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

    if chunksize:
        match_result_df = _chunksize_fuzzymatch_worker(
            worker_func=_lambda_fuzzymatch_threshold,
            chunksize=chunksize,
            query_list=cleaned_querying_list,
            choice_list=cleaned_choice_list
        )

    else:
        """if not chunksize is given"""
        match_result_df = _lambda_fuzzymatch_threshold(query_l=cleaned_querying_list,
                                                       choices_l=cleaned_choice_list)

    return match_result_df
