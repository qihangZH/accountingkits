import pandas as pd
import numpy as np


def suffix_remove_arr(listarrser, suffix_regex: str, **kwargs):
    """
    Input the suffix regex to remove the suffix, unlike pd.str.removesuffix
    Anything you input will seem like a group for regex
    :param listarrser: input array-like
    :param suffix_regex: regex string of suffix to remove
    :param kwargs:  pd.series.str.replace's other arguments;regex=True could not be changed
        BASED ON:https://pandas.pydata.org/docs/reference/api/pandas.Series.str.replace.html
    """

    # examine
    if (suffix_regex == '') or (not isinstance(suffix_regex, str)):
        raise ValueError('suffix_regex must be string and should not be ""')

    # core part
    temp_remove_suffix_arr = pd.Series(listarrser).astype(str).str.replace(
        '(' + suffix_regex + '){1}$', repl='', regex=True, **kwargs).values

    return temp_remove_suffix_arr


def extract_prefix_arr(listarrser, prefix_regex):
    """extract prefix by regex"""
    temp_extract_arr = listarrser.astype(str).str.extract(
        f'(^{prefix_regex}).*$', expand=False
    ).values  # choose

    return temp_extract_arr


def restore_leading_zeros_str_arr(listarrser, target_len):
    """
    Restore the lost leading zeros of the int/zeros list/arr/ser
    :param target_len: restore leading zeros to make new string back to target length.
    :param listarrser: INT list/array/ser which may lost leading zeros when saved
    :return: a string type array contains restored leading zeros
    """
    if np.all(pd.Series(listarrser).apply(lambda x: isinstance(x, int))):
        temp_string_ser = pd.Series(listarrser).astype(str)
    elif np.all(pd.Series(listarrser).apply(lambda x: isinstance(x, str))):
        # to judge that if the string of data are int
        temp_string_ser = pd.Series(listarrser).astype(int).astype(str)
    else:
        raise ValueError('Input array like should be int elements, or int strings')

    if np.nanmax(temp_string_ser.apply(len)) > target_len:
        raise ValueError('The max length of elements bigger than target length')

    dealed_string_ser = temp_string_ser.apply(
        lambda x: '0'*(target_len-len(x))+x
    )
    return dealed_string_ser


# -----------------------------------------------------------------------------------------
# """L1 Complex DataClean Functions"""#######################################################
# -----------------------------------------------------------------------------------------
def l1_suffix_and_around_remove_arr(listarrser, suffix_regex: str, around_regex: str, **kwargs):
    """
    REMOVE THE suffix, which ALSO remove all possible words or commas around it, like ',';'.';Space
    Anything you input will seem like a group for regex.
    WARNING: for set *, around_regex may work for ZERO TIME and CAUSE unintend results.
    :param listarrser: input array-like
    :param suffix_regex: regex string of suffix to remove
    :param around_regex: around words/comma regex, which are sepS BEFORE AND AFTER the target suffix.
    :param kwargs: pd.series.str.replace's other arguments-> case, flags, etc;regex=True could not be changed
        BASED ON:https://pandas.pydata.org/docs/reference/api/pandas.Series.str.replace.html
    """

    # examine
    if (suffix_regex == '') or (not isinstance(suffix_regex, str)):
        raise ValueError('suffix_regex must be string and should not be ""')

    # core part
    if around_regex == '':
        # temp_remove_suffix_arr = pd.Series(listarrser).astype(str).str.replace(
        #     '(' + suffix_regex + '){1}$', repl='', case=case, regex=True).values
        temp_remove_suffix_arr = suffix_remove_arr(
            listarrser=listarrser,
            suffix_regex=suffix_regex,
            **kwargs
        )
    else:
        temp_remove_suffix_arr = pd.Series(listarrser).astype(str).str.replace(
            f'({around_regex})*'+'(' + suffix_regex + '){1}' + f'({around_regex})*$',
            repl='',
            regex=True,
            **kwargs
        ).values

    return temp_remove_suffix_arr
