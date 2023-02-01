import accountingkits as ak
import pandas as pd
iris = pd.read_csv('https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv')
print(iris.apply(ak.Stats.SummarizeT.summary_quantile_ser, axis=0))
