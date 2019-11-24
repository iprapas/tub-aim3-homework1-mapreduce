import pandas as pd
results_df = pd.read_csv('../../../output_ex2/part-r-00000',sep='\t', header=None)
customers_df = pd.read_csv('./customer.csv',sep='|', header=None)
orders_df = pd.read_csv('./orders.csv',sep='|', header = None)

diff_custkey = set(customers_df[0]) - set(orders_df[1])
if set(customers_df[customers_df[0].isin(diff_custkey)][1]) == set(results_df[1]):
    print("Result verification: SUCCESS")
else:
    print("Result verification: FAILURE")