import numpy as np
import pandas as pd
import warnings


def summary_quantile_ser(arrser):
    """
    summary of a array-like data, use describe if work error.

    :param arrser: the array or ser to got the result, however list may works
    return: pd.Series, result of summary
    """
    try:
        return pd.Series({
            'Mean': np.nanmean(arrser),
            'Sd': np.nanstd(arrser),
            'p25': np.nanquantile(arrser, q=0.25),
            'Median': np.nanmedian(arrser),
            'p75': np.nanquantile(arrser, q=0.75)
        })
    except:
        warnings.warn('Some Error in selfmade, use pd.describe instead',
                      SyntaxWarning)
        return pd.Series(arrser).describe()

