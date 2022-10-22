# CSVファイルをPythonでDB(MySQL)にインポートする

IoT機器から出力されるさまざまな計測データが大量に出力されているが、近年ではDxの推進により、このようなデータをデータベースにインポートして分析するような要件が多くあると考える。

CSVをデータベースのテーブルにインポートする方法はいろいろあるが、ここでは近年よく使われるPythonを使って一番速くデータベース（MySQL）にインポートする方法を調査した件について投稿したい。

## 前提

| key      | value            |
|:---------|:-----------------|
|OS        | UBUNTU(WSL2)     |
|DBMS      | MySQL (8.0)      | 
|Language  | Python(3.10)     |
|framework | SQLAlchemy(1.4)  |

## 準備

### (1).モデル

従業員マスタのCSVをテーブルにインポートするサンプルとする。テーブル定義は以下の通り。

```sql
CREATE TABLE `t_employee` (
  `id` int NOT NULL,
  `name` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`)
)
```

### (2).CSVデータ

ＣＳＶファイルの文字コードは「utf-8」、改行コードは「LF(\n)」、囲み文字は「"(ダブルクォーテーション)」とします。10万行のデータを用意する。

```csv: employee.csv
"id","name"
"1","emp1"
"2","emp2"
"3","emp3"

・・・・

"99998","emp99999"
"99999","emp99999"
```

### (3).CSVデータの作成

テストデータを作成する処理をpythonで作ったので参考までに掲載しておく。

```python: output_csv.py
import csv

def get_writer(f):
    w = csv.writer(f,delimiter=',', # 区切り文字：カンマ
                    quotechar='"',  # 囲い文字：ダブルクォーテーション
                    lineterminator='\n',  # 改行コード：LF
                    quoting=csv.QUOTE_ALL)
    return w

with open('employee.csv', 'w') as f:
    w = get_writer(f)
    w.writerow(['id', 'name'])
    for i in range (1, 100000):
        w.writerow([i, f'emp{i}'])
```

### (4).install python libraries 

```
```

## 比較

ここでは2つの方法で処理時間を計測する。

### (1).Pandasを使う

pandasは便利なフレームワークで、DataFrameのto_sqlを使うことで簡単にCSVをテーブルにインポートできる。

```python: test1.py
import pandas as pd
from sqlalchemy import create_engine
df = pd.read_csv('employee.csv', encoding = 'utf-8')
user = 'testuser'
password = '********'
host = 'localhost'
db_name = 'testdb'
connection_url = f'mysql+mysqlconnector://{user}:{password}@{host}/{db_name}'
engine = create_engine(connection_url)
df.to_sql(con = engine, name = 't_employee', schema = 'testdb', index = False)
```

## (2).MySQLの「LOAD DATA INFILE」を使う

「LOAD DATA LOCAL INFILE」をsqlalchemyで実行する例。
接続文字列のオプションに「allow_local_infile=1」を付けている点に注意。

```python:
import os
from sqlalchemy import create_engine
user = 'testuser'
password = '********'
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
filepath = os.path.join(os.path.dirname(__file__), 'employee.csv')
wsql = sql.format(filepath, 't_employee')
engine = create_engine(connection_url)
with engine.connect() as conn:
    conn.execute(wsql)
```

「LOAD DATA INFILE」に指定するCSVがファイルは、データベースサーバー上にある必要がある。

## 3.結果

|test             |elapsed          |
|:----------------|----------------:|
|pandas           |22.07489681243897|
|load data infile | 1.68038845062256|

MySQLの「LOAD DATA INFILE」(当然の結果だが、)を使うほうが13倍以上の速度となった。

実際には、このように単純なインポートではなく、値を補完したり、カラムを加工してインポートしたりするような要件もあり、Pythonで処理をする必要もあったりするので、一概にこの方法にすればいいというものではないが、Pandasが便利過ぎて、よりよい方法を検討せずに、to_sqlを使って実装しがちになるが、単純なCSVをアップロードする処理はDBMSの機能を使うほうがよい。
