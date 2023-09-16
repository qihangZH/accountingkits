import typing
import numba
import numpy as np
import numpy.ma


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
                ij_argsort_picked_arr = ij_argsort_arr[:-1 * (extreme_num + 1):-1]
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


def mat_threshold_picking_arrtuple(data_mat: np.ndarray, index_arr: np.ndarray, columns_arr: np.ndarray,
                                   threshold: typing.Union[int, float], axis: typing.Literal[0, 1] = 1,
                                   threshold_pick_func=np.greater_equal,
                                   datatype=np.float64, parallel: bool = True
                                   ):
    """
    pick extreme nums, apply the threshold the axis you pick.
    for example, if axis=1, means you choose to apply to col
    :param threshold_pick_func: threshold_pick_func(x, threshold) pick the intend result, must used in numba funcs,
        see: https://numba.pydata.org/numba-doc/dev/reference/numpysupported.html
    :return : a tuple of array, index_arr, columns_arr, data_arr
    """

    """rename ind->row for sometimes index in this function mean others"""
    # trans the direction if axis = 0 (along with axis 0)
    if axis == 0:
        tmp_mat = data_mat.astype(datatype).T
        tmp_row = np.array(columns_arr)
        tmp_columns = np.array(index_arr)
    elif axis == 1:
        tmp_mat = data_mat.astype(datatype)
        tmp_row = np.array(index_arr)
        tmp_columns = np.array(columns_arr)
    else:
        raise ValueError('Must have axis be 0 or 1')

    @numba.njit(parallel=parallel)
    def _lambda_worker_pick_threshold_array(_mat):

        len_row, len_cols = _mat.shape

        # the container to save the position of theshold result
        threshold_bool_mat = np.zeros(
            shape=(len_row, len_cols),
            dtype=np.bool_
        )
        # we from this and know how many num each ind pick, leave a place for setting start index
        row_picknum_arr = np.zeros(len_row + 1, dtype=np.int32)

        for i in numba.prange(len_row):
            tmp_pickpos_arr = threshold_pick_func(_mat[i, :], threshold)

            threshold_bool_mat[i, :] = tmp_pickpos_arr
            row_picknum_arr[i + 1] = np.nansum(tmp_pickpos_arr)

        # after then we can accumulate and know the the index to save in the result arr

        row_pos_ind_arr = numpy.cumsum(row_picknum_arr)

        # add new container to save the position info of row
        result_argsort_row_arr = np.zeros(row_pos_ind_arr[-1], dtype=np.int32)
        # the container to save the arg-sort results(arg-sort-thresholds)
        result_argsort_col_arr = np.zeros(row_pos_ind_arr[-1], dtype=np.int32)

        result_data_arr = np.zeros(row_pos_ind_arr[-1], dtype=datatype)

        for i in numba.prange(len_row):
            # we have to know the position we place the results:
            start_ind = row_pos_ind_arr[i]
            end_end = row_pos_ind_arr[i + 1]

            result_argsort_row_arr[start_ind:end_end] = i
            result_argsort_col_arr[start_ind:end_end] = np.nonzero(threshold_bool_mat[i, :])[0]
            result_data_arr[start_ind:end_end] = _mat[i, threshold_bool_mat[i, :]]

        return result_argsort_row_arr, result_argsort_col_arr, result_data_arr

    tmp_row_argsort_arr, tmp_col_argsort_arr, tmp_result_data_arr = _lambda_worker_pick_threshold_array(_mat=tmp_mat)

    tmp_result_row_arr = tmp_row[tmp_row_argsort_arr]

    tmp_result_columns_arr = tmp_columns[tmp_col_argsort_arr]

    """
    finally, we have to reverse the index and columns for we
    actually reverse them in the first stage.
    """

    if axis == 1:
        return tmp_result_row_arr, tmp_result_columns_arr, tmp_result_data_arr
    else:
        return tmp_result_columns_arr, tmp_result_row_arr, tmp_result_data_arr
