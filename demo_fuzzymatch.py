# %%
from accountingkits.DataPrepare import FuzzyMatchT
import rapidfuzz
import pandas as pd
import numpy as np
from faker import Faker

# %%
if __name__ == '__main__':
    fake = Faker()
    Faker.seed(0)
    # Set the number of rows for each DataFrame
    num_rows = 300000  # You can adjust this as needed
    CHUNKSIZE = 5000
    query_sample_df = pd.DataFrame({
        'company_name': [fake.name() for _ in range(num_rows)],
        'naics': np.random.randint(1000, 2000, size=num_rows),  # Random integers between 1 and 10
        'state': np.random.choice(['A', 'B', 'C', 'D', 'E', 'S'], size=num_rows)  # Random choice from letters
    })
    choices_sample_df = pd.DataFrame({
        'company_name': [fake.name() for _ in range(num_rows)],
        'naics': np.random.randint(1000, 2000, size=num_rows),
        'state': np.random.choice(['A', 'B', 'C', 'D', 'E', 'Z'], size=num_rows)
    })

    print(FuzzyMatchT.list_fuzzymatch_max_df(querying_listarr=query_sample_df['company_name'],
                                             choice_list=choices_sample_df['company_name'],
                                             scorer=rapidfuzz.fuzz.ratio, chunksize=CHUNKSIZE
                                             ))

    #           origin_query      match_list  match_score
    # 0         Leah Schmidt    Leah Schmidt   100.000000
    # 1         Joshua Smith    Joshua Smith   100.000000
    # 2         Connor Smith    Connor Smith   100.000000
    # 3       Brenda Hartman  Brandy Hartman    85.714287
    # 4        Matthew White   Matthew White   100.000000
    # ...                ...             ...          ...
    # 159509      Noah Hodge    Norma Hodges    81.818184
    # 159510      Ryan Floyd      Ryan Floyd   100.000000
    # 159511     Bruce Allen     Bruce Allen   100.000000
    # 159512    Oscar Carter    Oscar Carter   100.000000
    # 159513    Taylor Glenn    Taylor Allen    83.333336
    # [159514 rows x 3 columns]

    """
    you can see the result is larger, for drop-duplicates are all having inside per loop
    because grouped, so we can assure duplicates drop less in grouped fuzzy

    By the while, many naic-state pair only have observe in one side, so it has less
    result then naics result.
    """

    print(FuzzyMatchT.group_fuzzymatch_df(
        list_fuzzymatch_func=FuzzyMatchT.list_fuzzymatch_max_df,
        query_group_df=query_sample_df,
        choices_group_df=choices_sample_df,
        groups=['naics', 'state'],
        query_col='company_name',
        choice_col='company_name',
        scorer=rapidfuzz.fuzz.ratio,
        loop_method='loop_in_parallel',
        chunksize=CHUNKSIZE
    ))

    #               origin_query        match_list  match_score  naics state
    # 0               Leah Moyer       Kyle Miller    47.619049   1000     B
    # 1           Matthew Wilson    Samuel Wilkins    50.000000   1000     B
    # 2          Kelly Hernandez   Robin Hernandez    66.666664   1000     B
    # 3             Richard Love     Michael Kline    48.000000   1000     B
    # 4          Rachel Cummings     Michael Arias    50.000000   1000     B
    # ...                    ...               ...          ...    ...   ...
    # 250065         Joshua King       Joshua Shaw    63.636364   1999     E
    # 250066  Christopher Barton  Christopher Moon    82.352943   1999     E
    # 250067       Krystal Smith     Gilbert Smith    61.538460   1999     E
    # 250068          Greg Owens    Gregory Phelps    58.333332   1999     E
    # 250069       Brian Chapman        Brian Hunt    60.869564   1999     E

    """you also could input string as in other..."""
    print(FuzzyMatchT.group_fuzzymatch_df(
        list_fuzzymatch_func=FuzzyMatchT.list_fuzzymatch_nlargests_df,
        query_group_df=query_sample_df,
        choices_group_df=choices_sample_df,
        groups='naics',
        query_col='company_name',
        choice_col='company_name',
        scorer=rapidfuzz.fuzz.ratio,
        loop_method='loop_in_parallel',
        chunksize=CHUNKSIZE,
        max_nums=3
    ))

    #          origin_query       match_list  match_score  naics
    # 0       Kenneth Brown    Anthony Braun    53.846153   1051
    # 1       Kenneth Brown       Kevin Barr    52.173912   1051
    # 2       Kenneth Brown  Katherine Jones    50.000000   1051
    # 3       Laura Spencer    Lawrence Ross    53.846153   1051
    # 4       Laura Spencer    Sarah Bennett    53.846153   1051
    # ...               ...              ...          ...    ...
    # 897070  Brian Sanders   Brandi Gardner    66.666664   1999
    # 897071  Brian Sanders    Brandi Harris    61.538460   1999
    # 897072     Adam Green   Angela Garrett    50.000000   1999
    # 897073     Adam Green  Dr. Andre Allen    48.000000   1999
    # 897074     Adam Green      William Lee    47.619049   1999
    #
    # [897075 rows x 4 columns]

    print(FuzzyMatchT.list_fuzzymatch_nlargests_df(querying_listarr=query_sample_df['company_name'],
                                                   choice_list=choices_sample_df['company_name'],
                                                   scorer=rapidfuzz.fuzz.ratio, chunksize=CHUNKSIZE,
                                                   max_nums=4
                                                   ))
    # 100%|██████████| 32/32 [03:24<00:00,  6.40s/it]
    #            origin_query       match_list  match_score
    # 0          Norma Fisher     Norma Fisher   100.000000
    # 1          Norma Fisher   Norman Fischer    92.307693
    # 2          Norma Fisher       Tom Fisher    81.818184
    # 3          Norma Fisher    Thomas Fisher    80.000000
    # 4        Jorge Sullivan   Jorge Sullivan   100.000000
    # ...                 ...              ...          ...
    # 636323     Karla Gordon     Carol Gordon    83.333336
    # 636324  Rachael Stevens  Rachael Stevens   100.000000
    # 636325  Rachael Stevens   Rachel Stevens    96.551727
    # 636326  Rachael Stevens  Michael Stevens    86.666664
    # 636327  Rachael Stevens  Rachel Stephens    86.666664
    #
    # [636328 rows x 3 columns]

    print(FuzzyMatchT.list_fuzzymatch_threshold_df(querying_listarr=query_sample_df['company_name'],
                                                   choice_list=choices_sample_df['company_name'],
                                                   scorer=rapidfuzz.fuzz.ratio, chunksize=CHUNKSIZE,
                                                   threshold=70))
    #   warnings.warn(
    # 100%|██████████| 32/32 [01:45<00:00,  3.30s/it]
    #              origin_query      match_list  match_score
    # 0            Norma Fisher   Thomas Fisher    80.000000
    # 1            Norma Fisher   Sarah Fischer    72.000000
    # 2            Norma Fisher    Laura Fisher    75.000000
    # 3            Norma Fisher    Debra Fisher    75.000000
    # 4            Norma Fisher   Rhonda Fisher    72.000000
    # ...                   ...             ...          ...
    # 20878834  Rachael Stevens    Rachel Owens    74.074074
    # 20878835  Rachael Stevens   Diane Stevens    71.428574
    # 20878836  Rachael Stevens   Rachel Reeves    78.571426
    # 20878837  Rachael Stevens  Bianca Stevens    75.862068
    # 20878838  Rachael Stevens  Rachael Deleon    75.862068
    #
    # [20878839 rows x 3 columns]
