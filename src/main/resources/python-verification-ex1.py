import pandas as pd
df = pd.read_csv('orders.csv', sep='|', header=None)
result_df = pd.read_csv('../../../output_ex1/part-r-00000', sep='\t', header=None)
print("Python result")
print(df[df[4]>='1993-10-01'].groupby(5)[3].sum())
print("MapReduce result")
print(result_df)