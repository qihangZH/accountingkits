# Accounting Kits

For any question, feel free to contact with me by e0952154@u.nus.edu

This is a self-made package which target is help to deal with different problems in accounting research.

## 1. Setup the package:

```shell
git clone https://github.com/qihangZH/accountingkits.git
cd accountingkits
pip install .
```

If developing need:

```shell
#IF DEVELOPING
pip install -e .
```

## 2. How if  I need to use the single module But I find it use other modules?

Nice question, If really so, you may have to replace the code for single modules sometimes only uses some _BasicFuncs functions.

For example in FuzzyMatchT.py:

```python
from .. import _BasicFunc
```

To search you can find that, "_BasicFunc" result contains:

```python
with pathos.multiprocessing.Pool(
                # for safer exception in multiprocess
                initializer=_BasicFunc.MultiprocessF.threads_interrupt_initiator
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



## 3.Other

This package also include the works https://github.com/r-boulland/Corporate-Website-Disclosure, I rewrite their functions to a new class in my kits.

References:

1. https://github.com/r-boulland/Corporate-Website-Disclosure 
