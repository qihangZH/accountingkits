import numpy as np
import pandas as pd
import warnings


def summary_quantile_ser(arrser):
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

