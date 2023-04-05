import pandas as pd
import sas7bdat
import chardet


def find_file_encoding(path):
    """
    find the encoding of target file(data or other else)
    more details could be seen: https://chardet.readthedocs.io/en/latest/usage.html;
    https://stackoverflow.com/questions/12468179/unicodedecodeerror-utf8-codec-cant-decode-byte-0x9c
    :param path: the path of file which encoding should be detected, the type will return as string.
    """
    with open(path, 'rb') as file:
        result = chardet.detect(file.read())

    return result['encoding']


def harte_hanks_txt_reader_2019_2021_df(PATH, **kwargs):
    """encoding='ISO-8859-1
    :param kwargs: other arguments should be passed to pd.read_csv()
    '"""
    temp_df = pd.read_csv(filepath_or_buffer=PATH, encoding="ISO-8859-1", low_memory=False, **kwargs)
    return temp_df


def harte_hanks_txt_reader_2017_2018_df(PATH, **kwargs):
    """encoding='UTF-16'
    :param kwargs: other arguments should be passed to pd.read_csv()
    """
    temp_df = pd.read_csv(filepath_or_buffer=PATH, encoding="UTF-16", low_memory=False, **kwargs)
    return temp_df


def harte_hanks_txt_reader_2016_df(PATH, **kwargs):
    """encoding='ISO-8859-1'
    :param kwargs: other arguments should be passed to pd.read_csv()
    """
    temp_df = pd.read_csv(filepath_or_buffer=PATH, encoding="ISO-8859-1", low_memory=False, **kwargs)
    return temp_df


def harte_hanks_txt_reader_1996_2015_df(PATH, **kwargs):
    """encoding='ISO-8859-1'
    :param kwargs: other arguments should be passed to pd.read_csv()
    """
    temp_df = pd.read_csv(
        filepath_or_buffer=PATH, encoding="ISO-8859-1", sep='\t', low_memory=False, **kwargs
    )
    return temp_df


def sas7bdat_reader_df(path, **kwargs):
    """
    read .sas7bdat data by sas7bdat.SAS7BDAT
    :param path: path to read, must have
    :param kwargs: other arguments of sas7bdat.SAS7BDAT(), see https://pyhacker.com/pages/sas7bdat.html
    """
    with sas7bdat.SAS7BDAT(path=path, **kwargs) as result:
        temp_df = result.to_data_frame()

    return temp_df


# -----------------------------------------------------------------------------------------
# """L1 Complex DataPrepare Functions"""#######################################################
# -----------------------------------------------------------------------------------------
def l1_hartehanks_readtxt_df(PATH, year: int, **kwargs):
    """
    :param PATH: read path
    :param year: be int i>=1996 should
    :param kwargs: other arguments should be passed to pd.read_csv()
    :return:
    """
    # dealing kwargs
    if (year >= 1996) and (year <= 2015):
        _temp_read_function = harte_hanks_txt_reader_1996_2015_df
    elif year == 2016:
        _temp_read_function = harte_hanks_txt_reader_2016_df
    elif (year >= 2017) and (year <= 2018):
        _temp_read_function = harte_hanks_txt_reader_2017_2018_df
    elif (year >= 2019) and (year <= 2021):
        _temp_read_function = harte_hanks_txt_reader_2019_2021_df
    else:
        raise ValueError(
            'year {} is not in function range, try again or change the read function'.format(year))

    return _temp_read_function(PATH, **kwargs)
