import pandas as pd
import numpy as np


def suffix_remove_arr(listarrser, suffix_regex,**kwargs):
    """
    Input the suffix regex to remove the suffix, unlike pd.str.removesuffix
    :param case: case to replace in the regex
    """
    case = kwargs['case'] if 'case' in kwargs else None

    temp_remove_suffix_arr = pd.Series(listarrser).astype(str).str.replace(
            '('+suffix_regex+'){1}$', repl='',case=case,regex=True).values

    return temp_remove_suffix_arr


def seps_suffix_remove_arr(listarrser, suffix_regex, **kwargs):
    """
    Auto remove the suffix, which also remove seps like ',';'.';Space
    :param kwargs:
    case -> case to replace in the regex;
    seps_regex -> reps regex, which are sep before and after suffix. default '(,)|(\.)|( )|(;)'
    """
    case = kwargs['case'] if 'case' in kwargs else None
    seps_regex = kwargs['seps_regex'] if 'seps_regex' in kwargs else '(,)|(\.)|( )|(;)'

    temp_remove_suffix_arr = pd.Series(listarrser).astype(str).str.replace(
        f'({seps_regex})+(' + suffix_regex + '){1}' + f'({seps_regex})*$',
        repl='',
        case=case, regex=True
    ).values

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
    if np.all(pd.Series(listarrser).apply(lambda x: isinstance(x,int))):
        temp_string_ser = pd.Series(listarrser).astype(str)
    elif np.all(pd.Series(listarrser).apply(lambda x: isinstance(x,str))):
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
