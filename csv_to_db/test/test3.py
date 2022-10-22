import os
from sqlalchemy import create_engine
import time

def main():
    user = 'testuser'
    password = 'testuser'
    host = 'localhost'
    db_name = 'testdb'
    connection_url = f'mysql+mysqlconnector://{user}:{password}@{host}/{db_name}?allow_local_infile=1'

    sqlx = """
LOAD DATA LOCAL INFILE '/home/user/.pyenv/versions/venv/tips/csv_to_db/test/環境依存文字.csv' INTO TABLE t_platform_dependent_chars FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\r\n' IGNORE 1 LINES    
"""
    engine = create_engine(connection_url)
    engine.execute("set character_set_database=cp932;")    
    with engine.begin() as t:
      t.execute(sqlx)

start = time.time()

main()

elapsed = time.time() - start
print(f'elapsed={elapsed}')
