import numpy as np
import pandas as pd


def summary_quantile_ser(arrser):
    return pd.Series({
        'Mean': np.nanmean(arrser),
        'Sd': np.nanstd(arrser),
        'p25': np.nanquantile(arrser, q=0.25),
        'Median': np.nanmedian(arrser),
        'p75': np.nanquantile(arrser, q=0.75)
    })
