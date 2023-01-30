import os
import numpy as np
import pandas as pd

from .. import _BasicFunc


def check_make_directory(check_make_dir):
    """Check and Make directory"""
    folder = os.path.exists(check_make_dir)

    if not folder:
        os.mkdir(check_make_dir)
        print('Make dir:{}'.format(check_make_dir))
    else:
        print('{} already exist.'.format(check_make_dir))


def check_make_directory_recursive(check_make_dir):
    """Check and Make directory, recursive and force"""
    folder = os.path.exists(check_make_dir)

    if not folder:
        os.makedirs(check_make_dir)
        print('Make dir:{}'.format(check_make_dir))
    else:
        print('{} already exist.'.format(check_make_dir))


def dir_colnames_csvdf(read_function, read_dir, save_path, suffix: str):
    """
    To show the colnames of Each DF in a dir, summary in to an DF
    No **kwargs because it only use in class and do not need it for the builders of functions

    :param read_function: the function to read the datas in the dir, function(dir+names)
    :param read_dir: the directory of the datas saved
    :param save_path: where to save the outcomes and the follow names of the output
    :param suffix: files of suffix path, however, should be type in "regex" to search.
    :return: dataframe of output cols
    """
    colname_dict = {}
    colname_len_dict = {}

    file_arr = np.array(os.listdir(read_dir))
    if not (suffix is None):
        pattern = '^.*' + suffix + '$'
        temp_choose_arr = pd.Series(file_arr).str.contains(pat=pattern, regex=True).values
        file_arr = file_arr[temp_choose_arr]
    else:
        pass

    print('FILENAMES:', file_arr)

    for i in file_arr:
        # read each
        colname_dict[i] = read_function(read_dir + i).columns.values
        colname_len_dict[i] = len(colname_dict[i])
    for i in file_arr:
        colname_dict[i] = _BasicFunc.ArrayLikeF.repunit_append_arr(
            target_len=max(colname_len_dict.values()),
            listarrser=colname_dict[i],
            repunit=np.nan)
    temp_df = pd.DataFrame(colname_dict).to_csv(save_path)

    return temp_df
