import pandas as pd


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


# -----------------------------------------------------------------------------------------
# """L1 Complex DataClean Functions"""#######################################################
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
