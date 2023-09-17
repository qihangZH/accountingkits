import itertools
from . import funcfactory

# -----------------------------------------------------------------------------------------
# Numba accelerator functions<instantiation>
# -----------------------------------------------------------------------------------------


# singlep/multip nextreme picker
funcdict_mat_peraxis_pick_nextreme = {
    arg: funcfactory.funcfactory_worker_pick_mat_nextreme(arg)
    for arg in [True, False]  # Is or not be parallel, parallel=True/False
}

funcdict_mat_peraxis_pick_threshold = {
    args: funcfactory.funcfactory_worker_pick_mat_threshold(*args)
    for args in itertools.product(
        [True, False],  # Is or not be parallel, parallel=True/False,
        ['greater_equal', 'greater', 'less', 'less_equal']
        # threshold_pick_method: typing.Literal['greater_equal', 'greater', 'less', 'less_equal']
    )
}
