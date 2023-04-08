import numpy as np
import pandas as pd
import copy
import typing


def na_percent_float(listarrser) -> float:
    temp_ser = pd.Series(listarrser)
    temp_len = len(temp_ser)
    na_percentage = sum(pd.isna(temp_ser)) / temp_len
    return na_percentage


def remove_duplicate_na_arr(listarrser):
    """remove the duplicate obs and NA obs"""
    temp_ser = pd.Series(listarrser).drop_duplicates().dropna()
    return temp_ser.to_numpy()


def latent_na_detector_bool_arr(listarrser,
                                na_regex,
                                case,
                                is_detect_explicit_na: bool,
                                is_specify_zerolen_na: bool
                                ):
    """
    detect latent na, like spaces or comma, only works for data which type is str
    :param listarrser: input
    :param na_regex: regex
    :param case: If True, case sensitive.
    :param is_detect_explicit_na: bool, True for detect explicit na, False for not detect
    :param is_specify_zerolen_na: bool, True for set zero length str as NA
    :return: bool array
    """
    if (not isinstance(is_detect_explicit_na, bool)) | (not isinstance(is_specify_zerolen_na, bool)):
        raise ValueError('is_detect_explicit_na/is_specify_zerolen_na only input for True or False')

    # fullmatch contains match works same here, Actually we need fullmatch here, however do not change
    na_detect_arr = pd.Series(listarrser).astype(str).str.match(
        # old version:
        # pat='^((,)|(\.)|( ))*('+na_regex+'){1}((,)|(\.)|( ))*$',
        pat=fr'^((,)|(\.)|(\s)|(;)|(\|))*({na_regex})?((,)|(\.)|(\s)|(;)|(\|))*$',
        case=case,
        na=is_detect_explicit_na  # substitute explicit n.a. with True/False
    ).to_numpy()

    # besides, we add another way to detect: string length==0 means na
    if is_specify_zerolen_na:
        zero_len_detect_arr = (pd.Series(listarrser).astype(str).apply(len) == 0).to_numpy()

        # both of them detect the need anwser.
        na_detect_arr = na_detect_arr | zero_len_detect_arr

    return na_detect_arr


def auto_substituite_duplicates_bychoices_arr(duplicates_listarrser, choice_listarrser,
                                              duplicate_keep: typing.Literal['first', 'last', False]):
    """
    Use choice list in choice_listarrser to help the duplicates substitute the found duplicates;
    However, Auto looping for no substitute result.
    :param duplicates_listarrser: target
    :param choice_listarrser:choices, length must same as target listarrser
    :param duplicate_keep: the method of pd.duplicates() keep, the True ones should be substituted
    :return: np.array
    """

    duplicates_arr = np.array(duplicates_listarrser)
    choice_arr = np.array(choice_listarrser)

    if len(choice_arr) != len(duplicates_arr):  # length check
        raise ValueError('length of duplicates_listarrser and choice_listarrser are not same')

    if pd.Series(duplicates_arr).nunique() != len(duplicates_arr):
        print('duplicates_listarrser has no duplicates')  # check duplicates
        return duplicates_arr

    if not (duplicate_keep is None):  # loop
        # Initialize
        temp_duplicates_boolean_arr = pd.Series(duplicates_arr).duplicated(keep=duplicate_keep).to_numpy()
        temp_nodup_remove_suffix_arr = copy.deepcopy(duplicates_arr)
        temp_nodup_remove_suffix_arr[temp_duplicates_boolean_arr] = choice_arr[temp_duplicates_boolean_arr]
        # other arguments
        _temp_loop_count = 0
        _temp_keep = duplicate_keep
        _temp_duplicate_count = sum(temp_duplicates_boolean_arr)
        print(f'Initialize autorestore duplicates last for:{sum(temp_duplicates_boolean_arr)},\n')
        # deal with duplicates
        while pd.Series(temp_nodup_remove_suffix_arr).nunique() != len(temp_nodup_remove_suffix_arr):

            last_loop_arr = temp_nodup_remove_suffix_arr
            temp_duplicates_boolean_arr = pd.Series(last_loop_arr
                                                    ).duplicated(keep=_temp_keep).to_numpy()
            temp_nodup_remove_suffix_arr = copy.deepcopy(last_loop_arr)
            temp_nodup_remove_suffix_arr[temp_duplicates_boolean_arr] = choice_arr[temp_duplicates_boolean_arr]
            _temp_loop_count += 1
            # check if in INFINITE LOOP: IF same, means last time dealing is meaningless
            if _temp_duplicate_count == sum(temp_duplicates_boolean_arr):
                if _temp_keep != False:
                    print('To stop infinite loop, change to False')
                    _temp_keep = False
                else:
                    raise ValueError('Problems in choice list, It cause infinite loop unsolvable')

            _temp_duplicate_count = sum(temp_duplicates_boolean_arr)
            print(f'auto_restore the duplicates...,loop {_temp_loop_count},\n')
            print(f'duplicates last for:{sum(temp_duplicates_boolean_arr)},\n')

        return temp_nodup_remove_suffix_arr
    else:
        raise ValueError('duplicate_keep can not be None, please reset')


# -----------------------------------------------------------------------------------------
# """level1 Complex DataPrepare Functions"""#################################################
# -----------------------------------------------------------------------------------------

def l1_latent_na_substitute_arr(listarrser, na_regex, case,
                                substitute_value: bool,
                                is_detect_explicit_na: bool,
                                ):
    """
    detect latent na, like spaces or comma, and substitute the whole element
    :param listarrser: input
    :param na_regex: regex
    :param case: If True, case sensitive.
    :param is_detect_explicit_na: bool, True for substitute explicit na, False for not substitute explicit na
    :param substitute_value: the substitute value to replace the detected na
    :param is_detect_explicit_na: bool, True for set zero length str as NA
    :return: bool array
    """
    detect_arr = latent_na_detector_bool_arr(listarrser=listarrser,
                                             na_regex=na_regex,
                                             case=case,
                                             is_detect_explicit_na=is_detect_explicit_na,
                                             is_specify_zerolen_na=is_detect_explicit_na
                                             )
    temp_arr = copy.deepcopy(np.array(listarrser))
    # before change, first copy
    temp_arr[detect_arr] = substitute_value
    return temp_arr
