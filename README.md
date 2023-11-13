# Accounting Kits

**Recommend CONDA manage the dependencies.**

This is a self-made package which target is help to deal with different problems in accounting research.


**WARNING: This version is Still PREVIEW and UNSTABLE! 
ANY functions and classes COULD BE CHANGED (NAMES OR OTHERS) IN  FUTURE!**

## 0. Developer Notes
1) name should be ```v\d.\d.\d.\d{6}[a-z]?```. For example, v0.1.1.230115 is for stable version. 
Where v0.1.3.231113b is notable for beta version. ```b``` suffix is means for beta and ```a``` is
for alpha. Or GitHub auto script will cause error.

## 1. Setup the package:

It is always recommend to get latest version from git directly.
```shell
git clone https://github.com/qihangZH/accountingkits.git
cd accountingkits
pip install .
```

OR get from pypi's latest release, may not latest version.

```shell
pip install accountingkits
```

If developing need(first download pack from git):

```shell
#IF DEVELOPING
python setup.py develop
```

Dependencies could be downloaded from git by requirements.txt easily:
It is not forced for you could management the dependency by conda.
```shell
pip install -r requirements.txt
```

## 2. How if  I need to use the single module But I find it use other modules?

Nice question, If really so, you may have to replace the code for single modules sometimes only uses some _BasicFuncs functions.

For example in FuzzyMatchT.py:

```python
from .. import _BasicTools
```

To search in FuzzyMatch.py,you can find that, "_BasicFunc" result contains:

```python
with pathos.multiprocessing.Pool(
        # for safer exception in multiprocess
        initializer=_BasicFunc.MultiprocessF.processes_interrupt_initiator
) as pool:
    ...
```

And the only function could be found:

```python
def threads_interrupt_initiator():
    """
    Each pool process will execute this as part of its
    initialization.
    Use this to keep safe for multiprocessing...and gracefully interrupt by keyboard
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)
```

To replace it, you can directly put it in your need module and add some your own codes, **however, I could not premise the _BasicFunc will not be refactored in future version.** 

```python
# copy here
def threads_interrupt_initiator():
    """
    Each pool process will execute this as part of its
    initialization.
    Use this to keep safe for multiprocessing...and gracefully interrupt by keyboard
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)

with pathos.multiprocessing.Pool(
                # for safer exception in multiprocess
                initializer=threads_interrupt_initiator
        ) as pool:
    ...
    
```

## 3. Deprecation and Future Warnings
**Any changes which cause Deprecation and Future Warnings will be placed here, mostly they cause version error.**

**If not, kindly send me email and I will show it in README.**

**However, Deprecation and Future warnings are unavailable for Preview/alpha/beta version**


## 4.References

1. This project includes code from the https://github.com/r-boulland/Corporate-Website-Disclosure, 
which is licensed under the MIT license. Precisely, the accountingkits/CrawlerApi/WayBackT.py
The full text of the MIT license can be found in the CrawlerT/LICENSE file.
