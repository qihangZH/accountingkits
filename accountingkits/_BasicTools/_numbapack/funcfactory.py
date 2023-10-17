import typing
import numba
import numpy as np
import numpy.ma


# -----------------------------------------------------------------------------------------
# Numba accelerator function-factory, for making the functions
# -----------------------------------------------------------------------------------------

def funcfactory_worker_pick_mat_nextreme(parallel: bool):
    @numba.njit(parallel=parallel)
    def _wrapped(_mat, extreme_num: int, nlargest: bool, datatype):

        len_ind, len_cols = _mat.shape

        # we have to make sure nlargest big the minium of len_cols and nlargest arg
        extreme_num = min(len_cols, extreme_num)

        # the container to save the arg-sort results(arg-sort-nextremes)
        result_argsort_row_arr = np.zeros(len_ind * extreme_num, dtype=np.int32)
        result_argsort_col_arr = np.zeros(len_ind * extreme_num, dtype=np.int32)
        result_data_arr = np.zeros(len_ind * extreme_num, dtype=datatype)

        for i in numba.prange(len_ind):
            ij_argsort_arr = np.argsort(_mat[i, :])

            if nlargest:
                ij_argsort_picked_arr = ij_argsort_arr[:-1 * (extreme_num + 1):-1]
            else:
                ij_argsort_picked_arr = ij_argsort_arr[:extreme_num:1]

            result_argsort_row_arr[i * extreme_num: (i + 1) * extreme_num] = i
            result_argsort_col_arr[i * extreme_num: (i + 1) * extreme_num] = ij_argsort_picked_arr
            result_data_arr[i * extreme_num: (i + 1) * extreme_num] = _mat[i, ij_argsort_picked_arr]

        return result_argsort_row_arr, result_argsort_col_arr, result_data_arr

    return _wrapped


def funcfactory_worker_pick_mat_threshold(
        parallel: bool,
        threshold_pick_method: typing.Literal['greater_equal', 'greater', 'less', 'less_equal']
):
    threshold_pick_func = {
        'greater_equal': np.greater_equal,
        'greater': np.greater,
        'less': np.less,
        'less_equal': np.less_equal
    }[threshold_pick_method]

    @numba.njit(parallel=parallel)
    def _wrapped(_mat, threshold: typing.Union[int, float], datatype):

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

    return _wrapped
