import asyncio
import time

import pandas as pd
import time
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import insert

from db_config import user, password, host, db_name

connection_url = f'mysql+mysqlconnector://{user}:{password}@{host}/{db_name}'


def insert_on_duplicate(table, conn, keys, data_iter):
    insert_stmt = insert(table.table).values(list(data_iter))
    on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(insert_stmt.inserted)
    conn.execute(on_duplicate_key_stmt)

def create_dataframe(url, encode):
    df = pd.read_csv(url, encoding = encode)
    return df

def register_to_db(engine, table_name, df):

    rows = df.to_sql(
        con = engine, 
        name = table_name, 
        schema = db_name, 
        if_exists = 'append', 
        index = False, 
        chunksize = 4096, 
        method=insert_on_duplicate)

    return rows

async def csv_to_db(csv_path, table_name, encoding='utf-8'):

    df = create_dataframe(csv_path, encoding)
    register_to_db(table_name, df)

def get_connection():
    engine = create_engine(connection_url)
    return engine

async def main(engine):
    tasks = [
        asyncio.create_task(csv_to_db(engine, p["csv"], p["table"], p["encoding"])) 
            for p in [
                {"csv":'employee.csv', "table": 't_employee', "encoding":'utf-8'},
            ]
    ]
    # r = [await task for talsk in tasks]
if __name__ == '__main__':
    start = time.time()
    engine = get_connection()
    asyncio.run(main(engine))
    elapsed = time.time() - start
    print(f'elapsed={elapsed}')
