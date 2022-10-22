import os
from sqlalchemy import create_engine
import time

def main():
    user = 'testuser'
    password = 'testuser'
    host = 'localhost'
    db_name = 'testdb'
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
    filepath=os.path.join(os.path.dirname(__file__), 'employee.csv')
    wsql = sql.format(filepath, 't_employee')
    engine = create_engine(connection_url)
    with engine.connect() as conn:
        conn.execute(wsql)

start = time.time()

main()

elapsed = time.time() - start
print(f'elapsed={elapsed}')
