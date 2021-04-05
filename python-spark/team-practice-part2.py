import myspark
from myspark import spark as spark
from pyspark.sql import functions as F
import math

df = spark.read.csv('C:/Users/JinHwan/Desktop/SK_Infosec_Academy/worked_files/MDproject4/join/connect.data/*.csv', header=True)
count_all = df.count()

QI = [
    '시도',
    '시군구',
    '고객등급'
]

df1 = df.groupby(*QI).count()
for ii in range(2, 5):
    k = df1.filter(f'count < {ii}').count()
    print(f'\tk-{ii} 불충분: {k}')
    if k == count_all:
        break

dfk = df1.filter('count >= 3').join(df, on=QI, how='left')
dfk.select(*QI, 'count').orderBy('count').show()

dfk.select(*QI).coalesce(1).write.mode('overwrite').csv('C:/Users/JinHwan/Desktop/SK_Infosec_Academy/worked_files/MDproject4/join/out-of-k3', header=True)
