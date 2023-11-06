import pandas as pd

df = pd.read_csv('../dataset/yellow_tripdata_2020-07.csv')
df.dropna(inplace=True)

df.columns = df.columns.str.replace('(?<=[a-z])(?=[A-Z])','_').str.lower()
# print(df.columns)

df = df[['vendor_id', 'passenger_count', 'trip_distance', 'payment_type','fare_amount','extra','mta_tax','tip_amount','tolls_amount','improvement_surcharge','total_amount','congestion_surcharge']]
df_top_passenger = df.sort_values('passenger_count',ascending=False).head(10)

print(df_top_passenger)