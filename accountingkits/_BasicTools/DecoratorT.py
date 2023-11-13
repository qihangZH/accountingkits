import time
import functools
import multiprocess.context
import pathos
from . import MultiprocessT


# --------------------------------------------------------------------------------------------
# functions level 0
# --------------------------------------------------------------------------------------------
def timer_wrapper(func):
    """Wrapper for function running timing"""

    @functools.wraps(func)
    def decorated(*args, **kwargs):
        time_start = time.time()
        result = func(*args, **kwargs)
        time_end = time.time()
        time_spend = time_end - time_start
        print('%s cost time: %.5f s' % (func.__name__, time_spend))
        return result

    return decorated


def timeout_process(max_runtime):
    """
    A decorator that runs a function in a separate process with a time limit.
    If the function exceeds the maximum runtime, it raises a TimeoutError.
    """

    def decorator(func):
        def __worker_timeout_process(*args, **kwargs):
            """
            This function is not a target for outside usage.
            It executes the function with given arguments and returns the result.
            """
            return func(*args, **kwargs)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with pathos.multiprocessing.Pool(initializer=MultiprocessT.processes_interrupt_initiator,
                                             processes=1) as pool:
                async_result = pool.apply_async(__worker_timeout_process, args, kwargs)

                try:
                    return async_result.get(timeout=max_runtime)
                except multiprocess.context.TimeoutError:
                    raise TimeoutError(
                        f"Function '{func.__name__}' exceeded maximum runtime of {max_runtime} seconds"
                    )

        return wrapper

    return decorator
