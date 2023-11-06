import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger, String, JSON, DateTime, Float, Integer, Boolean

def get_dataframe():
    file_parquet = "../dataset/yellow_tripdata_2023-01.parquet"
    df = pd.read_parquet(file_parquet)
    df.dropna(inplace=True)
    return df

def get_manipulate_data(df):
    df['VendorID'] = df['VendorID'].astype('int8')
    df['passenger_count'] = df['passenger_count'].astype('int8')
    df['RatecodeID'] = df['RatecodeID'].astype('int8')
    df['PULocationID'] = df['PULocationID'].astype('int8')
    df['DOLocationID'] = df['DOLocationID'].astype('int8')
    df['payment_type'] = df['payment_type'].astype('int8')

    df['store_and_fwd_flag'] = df['store_and_fwd_flag'].replace(['N','Y'], [False, True])
    df['store_and_fwd_flag'] = df['store_and_fwd_flag'].astype('boolean')

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    
    return df

def get_postgres_conn():
	user = 'postgres'
	password = 'admin'
	host = 'localhost'
	database = 'mydb'
	port = 5432

	conn_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'
	engine = create_engine(conn_string)
	return engine

def load_to_postgres(engine):
	df_schema = {
	'VendorID' : BigInteger,
	'tpep_pickup_datetime': DateTime,
	'tpep_dropoff_datetime': DateTime,
	'passenger_count': BigInteger,
	'trip_distance': Float,
	'RatedcodeID': BigInteger,
	'store_and_fwd_flag': Boolean,
	'PULocationID': BigInteger,
	'DOLocationID': BigInteger,
	'payment_type': BigInteger,
	'fare_amount': Float,
	'extra': Float,
	'mta_tax': Float,
	'tip_amount': Float,
	'tolls_amount': Float,
	'improvement_surcharge': Float,
	'total_amount': Float,
	'congestion_surcharge': Float,
	'airport_fee': Float
	}
	df.to_sql(name='alta', con=engine, if_exists='replace', index=False, schema='public', dtype=df_schema, method=None, chunksize=5000)
      

df = get_dataframe()
clean_data = get_manipulate_data(df)
print(clean_data.dtypes)

postgres_conn = get_postgres_conn()
load_to_postgres(postgres_conn)