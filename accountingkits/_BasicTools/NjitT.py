import typing
import numba
import numpy as np


# -----------------------------------------------------------------------------------------
# Numba accelerator functions
# -----------------------------------------------------------------------------------------

def mat_nextremes_picking_arrtuple(data_mat: np.ndarray, index_arr: np.ndarray, columns_arr: np.ndarray,
                                   extreme_num: int, axis: typing.Literal[0, 1] = 1, nlargest: bool = True,
                                   datatype=np.float64, parallel: bool = True
                                   ):
    """
    pick extreme nums, apply the n-extreme the axis you pick.
    for example, if axis=1, means you choose to apply

    :return : a tuple of array, index_arr, columns_arr, data_arr
    """

    # trans the direction if axis = 0 (along with axis 0)
    if axis == 0:
        tmp_mat = data_mat.astype(datatype).T
        tmp_index = np.array(columns_arr)
        tmp_columns = np.array(index_arr)
    elif axis == 1:
        tmp_mat = data_mat.astype(datatype)
        tmp_index = np.array(index_arr)
        tmp_columns = np.array(columns_arr)
    else:
        raise ValueError('Must have axis be 0 or 1')

    @numba.njit(parallel=parallel)
    def _lambda_worker_pick_nextreme_array(_mat):

        len_ind, len_cols = _mat.shape

        # the container to save the arg-sort results(arg-sort-nextremes)
        result_argsort_arr = np.zeros(len_ind * extreme_num, dtype=np.int32)
        result_data_arr = np.zeros(len_ind * extreme_num, dtype=datatype)

        for i in numba.prange(len_ind):
            ij_argsort_arr = np.argsort(_mat[i, :])

            if nlargest:
                ij_argsort_picked_arr = ij_argsort_arr[:-1 * (extreme_num+1):-1]
            else:
                ij_argsort_picked_arr = ij_argsort_arr[:extreme_num:1]

            result_argsort_arr[i * extreme_num: (i + 1) * extreme_num] = ij_argsort_picked_arr
            result_data_arr[i * extreme_num: (i + 1) * extreme_num] = _mat[i, ij_argsort_picked_arr]

        return result_argsort_arr, result_data_arr

    tmp_result_argsort_arr, tmp_result_data_arr = _lambda_worker_pick_nextreme_array(_mat=tmp_mat)

    # sequence: ind1 ind1 ind1(extreme_nums), ind2 ind2 ind2(extreme_nums), etc...

    tmp_result_index_arr = np.repeat(tmp_index[:, np.newaxis], extreme_num, axis=1).flatten(order='C')

    tmp_result_columns_arr = tmp_columns[tmp_result_argsort_arr]

    """
    finally, we have to reverse the index and columns for we
    actually reverse them in the first stage.
    """

    if axis == 1:
        return tmp_result_index_arr, tmp_result_columns_arr, tmp_result_data_arr
    else:
        return tmp_result_columns_arr, tmp_result_index_arr, tmp_result_data_arr
