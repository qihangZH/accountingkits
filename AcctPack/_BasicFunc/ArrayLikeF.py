import numpy as np
import pandas as pd


def test_allsame_bool(listarrser1, listarrser2):
    """
    test all be same
    :return: bool, is all same or not all same
    """
    temp_df = pd.DataFrame({'ser1': listarrser1, 'ser2': listarrser2})
    temp_df = temp_df.dropna(axis=0)
    temp_len = temp_df.shape[0]

    return np.nansum(temp_df.apply(func=lambda x: x['ser1'] == x['ser2'], axis=1).values) == temp_len


def repunit_append_arr(target_len, listarrser, repunit):
    """
    Append the repeat units to the end if the listarr to the target length
    :param target_len: target length of the list/array
    :param listarrser: input should be list/arr/ser
    :return: arr
    """
    temp_arr = np.array(listarrser) # even series change to array
    sup_arr = np.repeat(repunit, target_len - len(temp_arr))
    return np.concatenate([temp_arr, sup_arr], axis=0)
