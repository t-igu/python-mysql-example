import os
import asyncio
from sqlalchemy import create_engine
import sys
from db_config import user, password, host, db_name

import time
def record_time(func):
    def record(*args, **kwargs):
        start = time.time()
        ret = func(*args, **kwargs)
        elapsed_time = time.time() - start
        print('Elapsed time: {} [sec]'.format(elapsed_time), file=sys.stderr)
    return record

connection_url = f'mysql+mysqlconnector://{user}:{password}@{host}/{db_name}?allow_local_infile=1'

sql = """
LOAD DATA LOCAL INFILE '{}' REPLACE INTO TABLE {} 
FIELDS 
  TERMINATED BY ',' 
  ENCLOSED BY '"' 
LINES TERMINATED BY '\n' 
IGNORE 1 LINES
;
"""

async def load_data(conn, filename, table_name):
    file_path = get_filepath(filename)
    wsql = sql.format(file_path, table_name)
    print(wsql)
    conn.execute(wsql)

def get_filepath(file_name):
    filepath=os.path.join(os.path.dirname(__file__), file_name)
    return filepath

def get_connection():
    engine = create_engine(connection_url)
    return engine

async def main(conn):
    tasks = [
        asyncio.create_task(
            load_data(conn, p["csv"], p["table"])
        )
        for p in [
            {"csv":'employee.csv', "table": 't_employee'},
        ]
    ]
    # r = [await task for task in tasks]

if __name__ == '__main__':
    start = time.time()
    try:
        engine = get_connection()
        with engine.connect() as conn:
            asyncio.run(main(conn))
    finally:
        conn.close()    

    elapsed = time.time() - start
    print(f'elapsed={elapsed}')
