import os
import numpy as np
import pandas as pd
import warnings
from .. import _BasicTools


def check_make_directory(check_make_dir, force: bool = False):
    """
    Check and Make directory
    :param check_make_dir: the directory you hope to check-make
    :param force: is force to make the recursive dirs
    """
    folder = os.path.exists(check_make_dir)

    if not folder:
        if force:
            warnings.warn("\033[31myou already set 'force=True' for recursive, however unsafe\033[0m", UserWarning)
            temp_folder_maker = os.makedirs
        else:
            temp_folder_maker = os.mkdir

        temp_folder_maker(check_make_dir)

        print('Make dir:{}'.format(check_make_dir))
    else:
        print('{} already exist.'.format(check_make_dir))


def dir_colnames_df(read_df_func, read_dir, suffix_regex: str, **kwargs):
    """
    To show the colnames of Each DF in a dir, summary in to an DF

    :param read_df_func: the function to read the datas in the dir, function(dir+names),
        equally function(path), however,read_function must return pd.DataFrame...
    :param read_dir: the directory of the datas saved
    :param suffix_regex: suffix regex to search for files, however, should be type in "regex" to search.
        if you need to set Nothing, then simply use '', None, etc...
    :param kwargs: other arguments for suffix regex detect file names which pandas.Series.str.contains() use, see:
        https://pandas.pydata.org/docs/reference/api/pandas.Series.str.contains.html,
        however, regex=True,pat=patterns(special) can not be modified.
    :return: dataframe of output cols
    """
    colname_dict = {}
    colname_len_dict = {}

    file_arr = np.array(os.listdir(read_dir))

    # if we are sure that the suffix regex is string, then set it to suffix check.
    try:
        if (len(suffix_regex) != 0) and (isinstance(suffix_regex, str)):

            # pattern = '^.*(' + suffix_regex + '){1}$'

            """new version pattern"""
            pattern = fr'^.*({suffix_regex})$'

            temp_choose_arr = pd.Series(file_arr).str.contains(
                pat=pattern, regex=True, **kwargs
            ).to_numpy()
            file_arr = file_arr[temp_choose_arr]
        else:
            print('\033[31mfail in suffix regex using, \nhowever pass file suffix detecting, use all files\033[0m')
    except Exception as e:
        print(f'\033[31m{e} occurs in suffix regex using, \nhowever pass file suffix detecting, use all files\033[0m')

    # print the detect result of the function...
    print('FILENAMES:\n', file_arr)

    # get read_sample to ensure the readfunc return dataframe
    read_sample = read_df_func(read_dir + file_arr[0])
    if not isinstance(read_sample, pd.DataFrame):
        raise ValueError('read_function must return pandas.Dataframe, '
                         f'However, your read_function return {type(read_sample)}'
                         'reset read function!')

    for i in file_arr:
        # read each
        colname_dict[i] = read_df_func(read_dir + i).columns.to_numpy()
        colname_len_dict[i] = len(colname_dict[i])
    for i in file_arr:
        colname_dict[i] = _BasicTools.ArrayLikeT.repunit_append_arr(
            target_len=max(colname_len_dict.values()),
            listarrser=colname_dict[i],
            repunit=np.nan)

    temp_df = pd.DataFrame(colname_dict)

    return temp_df
