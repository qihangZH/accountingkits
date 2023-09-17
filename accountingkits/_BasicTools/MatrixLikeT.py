import typing
import numpy as np
from . import _numbapack


def mat_peraxis_nextremes_picking_arrtuple(data_mat: np.ndarray, index_arr: np.ndarray, columns_arr: np.ndarray,
                                           extreme_num: int, axis: typing.Literal[0, 1] = 1, nlargest: bool = True,
                                           datatype=np.float64, parallel: bool = True
                                           ):
    """
    pick extreme nums, apply the n-extreme the axis you pick.
    for example, if axis=1, means you choose to apply
    :param data_mat: data matrix
    :param index_arr: index array
    :param columns_arr: columns array
    :param extreme_num: the extreme number you choose, max N th or minimum N th,
    :param axis: apply along with X axis, from [0,1], 0 mean choose X extreme observations of each column.
        (along the axis 0)
    :param nlargest: bool, is ot not choose nlargest, else choose nsmallest
    :param datatype: the data format in calculation, default np.float64, or will cause error in numba
    :param parallel: Whether to use njit parallel
    :return : a tuple of array, index_arr, columns_arr, data_arr
    """

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

    tmp_row_argsort_arr, tmp_col_argsort_arr, tmp_result_data_arr = _numbapack.funcdict_mat_peraxis_pick_nextreme[
        parallel
    ](_mat=tmp_mat,
      extreme_num=extreme_num,
      nlargest=nlargest,
      datatype=datatype
      )

    # sequence: ind1 ind1 ind1(extreme_nums), ind2 ind2 ind2(extreme_nums), etc...

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


def mat_peraxis_threshold_picking_arrtuple(data_mat: np.ndarray, index_arr: np.ndarray, columns_arr: np.ndarray,
                                           threshold: typing.Union[int, float], axis: typing.Literal[0, 1] = 1,
                                           threshold_pick_method: typing.Literal[
                                               'greater_equal', 'greater', 'less', 'less_equal'] = 'greater_equal',
                                           datatype=np.float64, parallel: bool = True
                                           ):
    """
    pick extreme nums, apply the threshold the axis you pick.
    for example, if axis=1, means you choose to apply to col
    :param data_mat: data matrix
    :param index_arr: index array
    :param columns_arr: columns array
    :param threshold: the threshold of picking
    :param axis: apply along with X axis, from [0,1], 0 mean choose X extreme observations of each column.
        (along the axis 0)
    :param threshold_pick_method: threshold_pick_func(x, threshold) pick the result,
        ['greater_equal', 'greater', 'less', 'less_equal'] be chooses, default 'greater_equal'
    :param datatype: the data format in calculation, default np.float64, or will cause error in numba
    :param parallel: Whether to use njit parallel
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

    tmp_row_argsort_arr, tmp_col_argsort_arr, tmp_result_data_arr = _numbapack.funcdict_mat_peraxis_pick_threshold[
        parallel, threshold_pick_method
    ](
        _mat=tmp_mat,
        threshold=threshold,
        datatype=datatype
    )

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
