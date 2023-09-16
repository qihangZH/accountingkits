# %%
import pandas as pd
import numpy as np
from accountingkits._BasicTools import NjitT

# %%
if __name__ == '__main__':
    np.random.seed(0)
    # Set the number of rows for each DataFrame

    sample_index = np.arange(5)
    sample_columns = np.array(['A', 'B', 'C', 'D', 'E'])

    sample_mat = np.random.rand(len(sample_index), len(sample_columns))

    print(sample_mat)
    # [[0.5488135  0.71518937 0.60276338 0.54488318 0.4236548 ]
    #  [0.64589411 0.43758721 0.891773   0.96366276 0.38344152]
    #  [0.79172504 0.52889492 0.56804456 0.92559664 0.07103606]
    #  [0.0871293  0.0202184  0.83261985 0.77815675 0.87001215]
    #  [0.97861834 0.79915856 0.46147936 0.78052918 0.11827443]]

    rsts_tuple = NjitT.mat_nextremes_picking_arrtuple(
        data_mat=sample_mat,
        index_arr=sample_index,
        columns_arr=sample_columns,
        extreme_num=2,
        axis=1,
        nlargest=False
    )
    #    index columns      data
    # 0      0       E  0.423655
    # 1      0       D  0.544883
    # 2      1       E  0.383442
    # 3      1       B  0.437587
    # 4      2       E  0.071036
    # 5      2       B  0.528895
    # 6      3       B  0.020218
    # 7      3       A  0.087129
    # 8      4       E  0.118274
    # 9      4       C  0.461479

    print(
        pd.DataFrame(
            {
                'index': rsts_tuple[0],
                'columns': rsts_tuple[1],
                'data': rsts_tuple[2]
            }
        )
    )

    rsts_tuple = NjitT.mat_nextremes_picking_arrtuple(
        data_mat=sample_mat,
        index_arr=sample_index,
        columns_arr=sample_columns,
        extreme_num=3,
        axis=1,
        nlargest=True
    )
    #     index columns      data
    # 0       0       B  0.715189
    # 1       0       C  0.602763
    # 2       0       A  0.548814
    # 3       1       D  0.963663
    # 4       1       C  0.891773
    # 5       1       A  0.645894
    # 6       2       D  0.925597
    # 7       2       A  0.791725
    # 8       2       C  0.568045
    # 9       3       E  0.870012
    # 10      3       C  0.832620
    # 11      3       D  0.778157
    # 12      4       A  0.978618
    # 13      4       B  0.799159
    # 14      4       D  0.780529

    print(
        pd.DataFrame(
            {
                'index': rsts_tuple[0],
                'columns': rsts_tuple[1],
                'data': rsts_tuple[2]
            }
        )
    )

    rsts_tuple = NjitT.mat_nextremes_picking_arrtuple(
        data_mat=sample_mat,
        index_arr=sample_index,
        columns_arr=sample_columns,
        extreme_num=3,
        axis=0,
        nlargest=True
    )
    #     index columns      data
    # 0       4       A  0.978618
    # 1       2       A  0.791725
    # 2       1       A  0.645894
    # 3       4       B  0.799159
    # 4       0       B  0.715189
    # 5       2       B  0.528895
    # 6       1       C  0.891773
    # 7       3       C  0.832620
    # 8       0       C  0.602763
    # 9       1       D  0.963663
    # 10      2       D  0.925597
    # 11      4       D  0.780529
    # 12      3       E  0.870012
    # 13      0       E  0.423655
    # 14      1       E  0.383442

    print(
        pd.DataFrame(
            {
                'index': rsts_tuple[0],
                'columns': rsts_tuple[1],
                'data': rsts_tuple[2]
            }
        )
    )

    # %%

    rsts_tuple = NjitT.mat_threshold_picking_arrtuple(
        data_mat=sample_mat,
        index_arr=sample_index,
        columns_arr=sample_columns,
        threshold=0.6,
        axis=1,
        threshold_pick_func=np.greater
    )
    #     index columns      data
    # 0       0       B  0.715189
    # 1       0       C  0.602763
    # 2       1       A  0.645894
    # 3       1       C  0.891773
    # 4       1       D  0.963663
    # 5       2       A  0.791725
    # 6       2       D  0.925597
    # 7       3       C  0.832620
    # 8       3       D  0.778157
    # 9       3       E  0.870012
    # 10      4       A  0.978618
    # 11      4       B  0.799159
    # 12      4       D  0.780529

    print(
        pd.DataFrame(
            {
                'index': rsts_tuple[0],
                'columns': rsts_tuple[1],
                'data': rsts_tuple[2]
            }
        )
    )

    rsts_tuple = NjitT.mat_threshold_picking_arrtuple(
        data_mat=sample_mat,
        index_arr=sample_index,
        columns_arr=sample_columns,
        threshold=0.5,
        axis=1,
        threshold_pick_func=np.less
    )
    #   index columns      data
    # 0      0       E  0.423655
    # 1      1       B  0.437587
    # 2      1       E  0.383442
    # 3      2       E  0.071036
    # 4      3       A  0.087129
    # 5      3       B  0.020218
    # 6      4       C  0.461479
    # 7      4       E  0.118274

    print(
        pd.DataFrame(
            {
                'index': rsts_tuple[0],
                'columns': rsts_tuple[1],
                'data': rsts_tuple[2]
            }
        )
    )

    rsts_tuple = NjitT.mat_threshold_picking_arrtuple(
        data_mat=sample_mat,
        index_arr=sample_index,
        columns_arr=sample_columns,
        threshold=0.5,
        axis=0,
        threshold_pick_func=np.less
    )
    #    index columns      data
    # 0      3       A  0.087129
    # 1      1       B  0.437587
    # 2      3       B  0.020218
    # 3      4       C  0.461479
    # 4      0       E  0.423655
    # 5      1       E  0.383442
    # 6      2       E  0.071036
    # 7      4       E  0.118274

    print(
        pd.DataFrame(
            {
                'index': rsts_tuple[0],
                'columns': rsts_tuple[1],
                'data': rsts_tuple[2]
            }
        )
    )
