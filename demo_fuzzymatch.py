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

    #            origin_query         match_list  match_score  naics state
    # 0           Albert Mack        Laura Black    45.454544   1000     B
    # 1          James Miller         Jake Avery    54.545456   1000     B
    # 2           Sarah Marks     Sandra Morales    64.000000   1000     B
    # 3        Jessica Branch  Jessica Singh DDS    64.516129   1000     B
    # 4         Alexa Mahoney     Angela Mahoney    81.481483   1000     B
    # ...                 ...                ...          ...    ...   ...
    # 249850      Danny Kelly   Anthony Williams    44.444443   1999     E
    # 249851      Jack Watson       Jason Fisher    43.478260   1999     E
    # 249852  William Stewart   Anthony Williams    45.161289   1999     E
    # 249853      Amy Edwards      Johnny Harris    41.666668   1999     E
    # 249854    Krystal Brown   Crystal Gonzalez    62.068966   1999     E
    #
    # [249855 rows x 5 columns]

    """you also could input string as in other..."""
    print(FuzzyMatchT.group_fuzzymatch_df(
        list_fuzzymatch_func=FuzzyMatchT.list_fuzzymatch_max_df,
        query_group_df=query_sample_df,
        choices_group_df=choices_sample_df,
        groups='naics',
        query_col='company_name',
        choice_col='company_name',
        scorer=rapidfuzz.fuzz.ratio,
        loop_method='loop_in_parallel',
        chunksize=CHUNKSIZE
    ))

    #                      origin_query            match_list  match_score  naics
    # 0                  Maria Sims          Marvin James    63.636364   1001
    # 1              Melanie Gaines       Melanie Sweeney    68.965515   1001
    # 2               Thomas Turner       Thomas Hart DDS    57.142857   1001
    # 3            Charles Thompson        Billy Thompson    66.666664   1001
    # 4                Andrea Dixon          Andrea Brown    75.000000   1001
    # ...                       ...                   ...          ...    ...
    # 299011             Jose Payne            Jose Stone    70.000000   1999
    # 299012            Amber Davis           Becky Davis    63.636364   1999
    # 299013            April Smith         Patrick Smith    66.666664   1999
    # 299014       Jennifer Baldwin     Jennifer Williams    66.666664   1999
    # 299015  Christopher Wilson MD  Christopher Garrison    78.048782   1999
    #
    # [299016 rows x 4 columns]
