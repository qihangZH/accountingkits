import tqdm
import numpy as np
import pandas as pd
import pathos
import difflib
import rapidfuzz
import time
from thefuzz import process as thefuzz_process
from .. import _BasicFunc


def list_fuzzymatching_df(querying_listarr, choice_list, method, scorer):
    """
    lisklikearray->fuzzymatch with a LIST. scorer I prefer rapidfuzz.fuzz.ratio(Normalize levenshtein)
    :param querying_listarr:~
    :param choice_list:~
    :param method: npapply/multiprocessing/rapidfuzz_cdist/difflib,difflib use special method to match, Not levenshtein
    :param scorer: the scorer of fuzzymatch, it could be changed to different result,
            classic normalized_levenshtein be rapidfuzz.fuzz.ratio,however be useless in difflib method
    :return: result dataframe
    """

    # 0.1.1.230205_alpha-> new version only check the n.a. of query list, automatically remove choices duplicates/na >>>
    """
    deprecated old version, 0.1.1.230203_alpha
    
    if (sum(pd.Series(querying_listarr).isna()) != 0) | (sum(pd.Series(choice_list).isna()) != 0):
        raise ValueError(
            "Error:querying_listarr/choice_list are not ZERO NULL/NAN OBS:'\n" +
            f"querying_listarr {sum(pd.Series(querying_listarr).isna())} NA," +
            f" choice_list {sum(pd.Series(choice_list).isna())} NA"
        )
    
    if np.any(pd.Series(choice_list).duplicated()):
        raise ValueError('Error:there are duplicated in choice list, may cause confusing result')
    """

    if np.any(pd.Series(querying_listarr).isna()):
        raise ValueError("\033[31mNull/Na in querying list may cause confounding\033[0m")

    if np.any(pd.Series(choice_list).duplicated()) or np.any(pd.Series(choice_list).isna()):
        print(
            "\nDuplicates or Null/Na in choices are detected and automatically removed\n"
        )
        choice_list = pd.Series(choice_list).dropna().drop_duplicates().to_list()
    # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

    # set constants
    """CONSTANTS CAN NOT BE CHANGED"""
    CONST_querying_listarr = pd.Series(querying_listarr).astype(str).tolist()
    CONST_choice_list = pd.Series(choice_list).astype(str).tolist()

    def __LAMBDA_taskof_list_fuzzymatching_list_tuple(identifier):
        """
        :param identifier:identifier
        :return: results tuple
        """
        value = thefuzz_process.extractOne(
            query=CONST_querying_listarr[identifier],
            choices=CONST_choice_list,
            scorer=scorer
        )
        return (value, identifier)

    """Dropped, for even slower then npapply and totally unstable, WRONG OUTPUTS MOSTTIMES"""
    # def __LAMBDA_taskof_list_rapidFM_list_tuple(identifier):
    #     """
    #     :param identifier:identifier
    #     :return: results tuple
    #     """
    #     value = rapidfuzz.process.extractOne(query=CONST_querying_listarr[identifier],
    #                                         #Only Choose first two rows
    #                                          choices=CONST_choice_list,
    #                                          scorer=scorer
    #                                          )[:2]
    #     return (value,identifier)

    def __LAMBDA_taskof_list_difflibmatching_list_tuple(identifier):
        """
        :param identifier:
        :return:
        """
        temp_input = CONST_querying_listarr[identifier]
        match_result = difflib.get_close_matches(word=temp_input, possibilities=CONST_choice_list, n=1)
        if match_result == []:
            match_result = ''
        else:
            match_result = match_result[0]
        match_scores = difflib.SequenceMatcher(None, match_result, temp_input).ratio()
        #  *100
        value = (match_result, match_scores)

        return (value, identifier)

    if method == 'npapply':
        npapply_query_arr = np.array(CONST_querying_listarr)[:, np.newaxis]

        time_start = time.time()
        npapply_match_result_mat = np.apply_along_axis(
            func1d=lambda x: thefuzz_process.extractOne(query=x[0], choices=CONST_choice_list, scorer=scorer),
            arr=npapply_query_arr, axis=1)
        time_end = time.time()
        print('list_fuzzymatching_list method {} time cost'.format(method), time_end - time_start, 's')

        #  In after place match mean result from choice list
        #  Only Choose first two rows
        match_result_df = pd.DataFrame(npapply_match_result_mat, columns=['match_list', 'match_score'])

    elif method in ['multiprocessing', 'difflib']:
        #  Slow when compare with others, However is useful
        _temp_loop_function = {
            'multiprocessing': __LAMBDA_taskof_list_fuzzymatching_list_tuple,
            # 'rapidfuzz_multiprocessing': __LAMBDA_taskof_list_rapidFM_list_tuple,
            'difflib': __LAMBDA_taskof_list_difflibmatching_list_tuple
        }[method]

        temp_matching_list = []
        temp_score_list = []
        temp_identifier_list = []

        with pathos.multiprocessing.Pool(
                # for safer exception in multiprocess
                initializer=_BasicFunc.MultiprocessF.threads_interrupt_initiator
        ) as pool:
            for result in tqdm.tqdm(
                    pool.imap_unordered(
                        _temp_loop_function,
                        range(len(CONST_querying_listarr))
                    ),
                    total=len(CONST_querying_listarr)
            ):
                temp_matching_list.append(result[0][0])
                temp_score_list.append(result[0][1])
                temp_identifier_list.append(result[1])

        #  In after place match mean result from choice list
        mp_match_result_df = pd.DataFrame(
            {
                'match_list': temp_matching_list,
                'match_score': temp_score_list,
                'identifier': temp_identifier_list

            }
        )
        mp_match_result_df = mp_match_result_df.sort_values(by='identifier', ascending=True)
        match_result_df = mp_match_result_df[['match_list', 'match_score']].reset_index().copy()
        del match_result_df['index']

    elif method == 'rapidfuzz_cdist':
        """SuperFast Cpp Matrix method for matching result"""
        time_start = time.time()
        matchscores_total_mat = rapidfuzz.process.cdist(queries=CONST_querying_listarr,
                                                        scorer=scorer,
                                                        choices=CONST_choice_list,
                                                        workers=-1)
        matchscores_position_arr = np.nanargmax(matchscores_total_mat, axis=1)
        # In after place match mean result from choice list
        match_result_df = pd.DataFrame(
            {
                'match_list': np.array(CONST_choice_list)[matchscores_position_arr],
                'match_score': matchscores_total_mat[np.arange(len(CONST_querying_listarr)),
                                                     matchscores_position_arr],
            }
        )
        time_end = time.time()
        print('list_fuzzymatching_list method {} time cost'.format(method), time_end - time_start, 's')
    else:
        raise ValueError('Wrong method of iteration')

    match_result_df = match_result_df.copy()
    match_result_df['match_score'] = match_result_df['match_score'].astype(float)
    # make 0~100%
    if np.all((match_result_df['match_score'].values >= 0) &
              (match_result_df['match_score'].values <= 1)):
        match_result_df['match_score'] = np.multiply(match_result_df['match_score'], 100)
        print(f'method {method},scorer {scorer} has *100 their scores')

    return match_result_df


# ----------------------------------------------------------------------------------------------------------------------
# """L1 Complex DataPrepare Functions"""################################################################################
# ----------------------------------------------------------------------------------------------------------------------


def l1_auto_fuzzymatching_df(querying_listarr, choice_list, slicing_len, method, scorer):
    """
    slicing for memory saving....
    lisklikearray->fuzzymatch with a LIST.scorer I prefer rapidfuzz.fuzz.ratio(Normalize levenshtein)
    :param slicing_len: slicing len for each matching(rows)
    :param querying_listarr:~
    :param choice_list:~
    :param method: npapply/multiprocessing/rapidfuzz_cdist/difflib
                ,difflib use special method to match, Not levenshtein
    :param scorer: the scorer of fuzzymatch, it could be changed to different result,
            classic normalized_levenshtein be fuzz.ratio,however be useless in difflib method
    :return: result dataframe
    """

    # 0.1.1.230205_alpha-> new version only check the n.a. of query list, automatically remove choices duplicates/na >>>

    # add this only for not raise Value-error after running long-time script
    if np.any(pd.Series(querying_listarr).isna()):
        raise ValueError("\033[31mAuto Function->Null/Na in querying list may cause confounding\033[0m")

    # add this to avoid waste of performance and consistency
    if np.any(pd.Series(choice_list).duplicated()) or np.any(pd.Series(choice_list).isna()):
        print(
            "\nDuplicates or Null/Na in choices are detected and automatically removed\n"
        )
        choice_list = pd.Series(choice_list).dropna().drop_duplicates().to_list()
    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

    querying_arr = np.array(querying_listarr)
    # slicing for memory easier use.
    slice_arr = np.arange(0, len(querying_arr), slicing_len)
    print(
        'TOTAL LENGTH of none SLICED data:{},SLICES be {} seps'.format(len(querying_arr), len(slice_arr))
    )
    if slice_arr[-1] != len(querying_arr):
        slice_arr = np.append(slice_arr, len(querying_arr))   # his step is run in 100% but not remove 'if'

    df_concat_list = []
    for k in range(len(slice_arr) - 1):
        print('MATCH SLICE:{}~{}'.format(slice_arr[k], slice_arr[k + 1]))
        querying_k_arr = querying_arr[slice_arr[k]:slice_arr[k + 1]]
        matchresult_k_df = list_fuzzymatching_df(querying_k_arr, choice_list, method=method, scorer=scorer)

        df_concat_list.append(matchresult_k_df)

    total_df = pd.concat(df_concat_list, axis=0)
    return total_df
