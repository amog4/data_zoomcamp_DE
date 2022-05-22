import sys
import pandas as pd
from sqlalchemy import create_engine
import time
import argparse


def main(args):
    host = args.host
    username = args.username
    password = args.password
    database = args.database
    table = args.table
    url = args.url

    engine = create_engine(f"postgresql://{host}:5432/{database}?user={username}&password={password}")
    conn  = engine.connect()

    pdf = pd.read_parquet(f"{url}",engine = 'pyarrow')

    schema = pd.io.sql.get_schema(pdf,name =  'yellow_taxi_data',con = conn)

    pdf.tpep_dropoff_datetime = pd.to_datetime(pdf['tpep_dropoff_datetime'])
    pdf.tpep_pickup_datetime = pd.to_datetime(pdf['tpep_pickup_datetime'])

    pdf.head(0).to_sql(name = 'yellow_taxi_data',con = conn,if_exists='replace')
    start_time = time.time()
    for r in range(10000,pdf.shape[0],10000):
        s = r-10000
        pdf.iloc[s:r,:].to_sql(name = 'yellow_taxi_data',con = conn,if_exists='append')
        print(f'Time taken %{0}.3f sec and processed {r} rows'% (round((time.time() - start_time),3)))
        

    print('job completed')


if __name__ == '__main__':


    parser = argparse.ArgumentParser()

    parser.add_argument("--host",type=str, required=True, help="DB host")
    parser.add_argument("--username",type=str, required=True, help="DB username")
    parser.add_argument("--password",type=str, required=True, help="DB password")
    parser.add_argument("--database",type=str, required=True, help="DB host")
    parser.add_argument("--table",type=str, required=True, help="DB host")
    parser.add_argument("--url",type=str, required=True, help="url")
    args = parser.parse_args()

    print(args)
    main(args)



