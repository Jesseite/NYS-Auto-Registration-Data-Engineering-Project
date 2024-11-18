import pandas as pd
import os
import psycopg2
from dotenv import load_dotenv

#Loading variables from .env file
load_dotenv()

#Load the transformed data file
df = pd.read_pickle('transformed_data.pkl')

#Connect to PostgreSQL
try:
    conn = psycopg2.connect(
        user = os.environ.get('POSTGRES_USER'),
        host = os.getenv('POSTGRES_HOST'),
        password = os.getenv('POSTGRES_PASSWORD'),
        database = 'dbspace'
    )
    #Connection test
    cur = conn.cursor()
    print('Connection successfully')
except Exception as e:
    print(f'Connection failed: {e}')

#Insert data into PostgreSQL
try: 
    for index, row in df.iterrows():
        cur.execute("""
        INSERT INTO nys_registration (vin, city, state, zip, county, model_year, make, body_type, fuel_type, unladen_weight, reg_valid_date, reg_expiration_date, color) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (vin) DO NOTHING; -- Use ON CONFLICT to handle duplicates
        """, (row['vin'], row['city'], row['state'], row['zip'], row['county'], row['model_year'], row['make'], row['body_type'], row['fuel_type'], row['unladen_weight'], row['reg_valid_date'], row['reg_expiration_date'], row['color'])
        )
    conn.commit()
    print('Data loaded into PostgreSQL successfully')
except Exception as e:
    print(f'Data load failed: {e}')

#Close the connection
cur.close()
conn.close()




















"""
#Connect to PostgreSQL
engine = create_engine(f'postgresql+psycopg2://{os.getenv("POSTGRES_USER")}:{os.getenv("POSTGRES_PASSWORD")}@{os.getenv("POSTGRES_HOST")}:{os.getenv("POSTGRES_PORT")}/dbspace')

#Insert the dataframe into Postgres
try:
    df.to_sql('nys_registration', con=engine, if_exists='replace', index=False)
    print("Data loaded into Postgres successfully")
except Exception as e:
    print(f"Data load failed: {e}")"""