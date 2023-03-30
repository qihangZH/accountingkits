import numpy as np
import pandas as pd


def sic4_fininst_select_bool_arr(listarrser):
    # 2022/12/15
    # temp_arr = pd.Series(listarrser).astype('int32').values
    # Set again:
    # pandas.errors.IntCastingNaNError: Cannot convert non-finite values (NA or inf) to integer
    temp_ser = pd.Series(listarrser).copy()
    bool_not_na_ser = ~(temp_ser.isna())
    temp_ser[bool_not_na_ser] = (temp_ser[bool_not_na_ser]).apply('int32')

    bool_arr = (np.greater_equal(temp_ser.values, 6000) & np.less_equal(temp_ser.values, 6999))
    return bool_arr


def naic6_fininst_select_bool_arr(listarrser):
    # Set again:
    # pandas.errors.IntCastingNaNError: Cannot convert non-finite values (NA or inf) to integer
    temp_ser = pd.Series(listarrser).copy()
    bool_not_na_ser = ~(temp_ser.isna())
    temp_ser[bool_not_na_ser] = (temp_ser[bool_not_na_ser]).apply('int32')

    bool_arr = (np.greater_equal(temp_ser.values, 520000) & np.less_equal(temp_ser.values, 529999))
    return bool_arr


def int_rangeselect_bool_arr(listarrser, floor: int, ceil: int,
                             include_floor_bool: bool,
                             include_ceil_bool: bool
                             ):
    """
    Universal function,select bool array from already exist listarrser, Likely to "NAIC_bank_select_bool_arr"

    :param listarrser: the query listarrser(arraylike)
    :param floor:the floor of the range selection, be int
    :param ceil: the ceil of the range selection, be int
    :param include_floor_bool: include floor or not
    :param include_ceil_bool: include ceil or not
    :return: bool arr for selection outcomes
    """
    if isinstance(include_floor_bool, bool) and isinstance(include_ceil_bool, bool):
        pass
    else:
        raise ValueError('include_floor_bool,include_ceil_bool should be bool')

    temp_floor_func = np.greater_equal if include_floor_bool else np.greater
    temp_ceil_func = np.less_equal if include_ceil_bool else np.less
    # Set again:
    # pandas.errors.IntCastingNaNError: Cannot convert non-finite values (NA or inf) to integer
    temp_ser = pd.Series(listarrser).copy()
    bool_not_na_ser = ~(temp_ser.isna())
    temp_ser[bool_not_na_ser] = (temp_ser[bool_not_na_ser]).apply('int32')

    bool_arr = (temp_floor_func(temp_ser.values, floor) & temp_ceil_func(temp_ser.values, ceil))
    return bool_arr


# -----------------------------------------------------------------------------------------
# """L1 Complex DataPrepare Functions"""#######################################################
# -----------------------------------------------------------------------------------------


def l1_fininst_sic4naic6_selectbool_arr(SIC_listarrser, NAIC_listarrser, combine_method):
    """
    :param SIC_listarrser: If None,then set None, or listarrser
    :param NAIC_listarrser: If None,then set None, or listarrser
    :param combine_method: how to combine the result if input SIC and NAIC together in the SAME INPUT."and',"or"
    :return: np.array
    """
    if (SIC_listarrser is None) and (NAIC_listarrser is None):
        raise ValueError('You have to input least SIC/NAIC')
    elif (not (SIC_listarrser is None)) and (not (NAIC_listarrser is None)):
        sic_bool_arr = sic4_fininst_select_bool_arr(SIC_listarrser)
        naic_bool_arr = naic6_fininst_select_bool_arr(NAIC_listarrser)
        if combine_method == 'and':
            bool_arr = sic_bool_arr & naic_bool_arr
        elif combine_method == 'or':
            bool_arr = sic_bool_arr | naic_bool_arr
        else:
            raise ValueError('combine_method Invalid,input "and","or" instead')
    elif not (SIC_listarrser is None):
        bool_arr = sic4_fininst_select_bool_arr(SIC_listarrser)
    elif not (NAIC_listarrser is None):
        bool_arr = naic6_fininst_select_bool_arr(NAIC_listarrser)
    else:
        raise ValueError('Unexpected input')

    return bool_arr
