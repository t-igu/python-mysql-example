import pandas as pd
from sqlalchemy import create_engine
import time

def main():

    df = pd.read_csv('employee.csv', encoding = 'utf-8')
    user = 'testuser'
    password = 'testuser'
    host = 'localhost'
    db_name = 'testdb'
    connection_url = f'mysql+mysqlconnector://{user}:{password}@{host}/{db_name}'
    engine = create_engine(connection_url)
    df.to_sql(
            con = engine, 
            name = 't_employee', 
            schema = 'testdb', 
            if_exists = 'append', 
            index = False, 
            chunksize = 4096)

start = time.time()

main()

elapsed = time.time() - start
print(f'elapsed={elapsed}')
