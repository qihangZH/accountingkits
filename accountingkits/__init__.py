from . import DataPrepare
from . import OSKit
from . import CrawlerApi
from . import _BasicTools

try:
    from ._version import __version__
except ModuleNotFoundError as e:
    # if it is download from git directly
    __version__ = 'v.latest.preview.version'

"""
package principle:
    1. Input and output should end with input/return type
       Like list/arr/df/bool_arr/dict/int/float/str...
    2. constant vars should be UPPER_CASE, others be snake_case
    3. IMPORTANT: ALL CHANGEABLE TYPE(df,array,list) should 
        DEEP COPY before CHANGED IN MEMORY!
"""
