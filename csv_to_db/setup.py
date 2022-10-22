import csv

def get_writer(f):
    w = csv.writer(f,delimiter=',', # 区切り文字はカンマ
                    quotechar='"',  # 囲い文字はダブルクォーテーション
                    lineterminator='\n',
                    quoting=csv.QUOTE_ALL)
    return w

with open('employee.csv', 'w') as f:
    w = get_writer(f)
    w.writerow(['id', 'name'])
    for i in range (1, 100000):
        w.writerow([i, f'emp{i}'])

