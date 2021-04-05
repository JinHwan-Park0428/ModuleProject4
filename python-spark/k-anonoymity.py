import myspark
from myspark import spark as spark
from pyspark.sql import functions as F
import math

df = spark.read.csv('C:/Users/JinHwan/data/med.csv', header=True)
count_all = df.count()

year__ = F.udf(lambda x: x[:4], 'string')
df = df.withColumn('birth_year', year__(df['생년월일']))
df = df.withColumn('diag_year', year__(df['진단일자']))
df = df.withColumn('in_year', year__(df['입원일자']))
df = df.withColumn('out_year', year__(df['퇴원일자']))
df = df.withColumn('신장', df['신장'].cast('int'))
df = df.withColumn('체중', df['체중'].cast('int'))
cat_10__ = F.udf(lambda x: math.floor(x / 10) * 10, 'int')
df = df.withColumn('height', cat_10__(df['신장']))
df = df.withColumn('weight', cat_10__(df['체중']))

height_local__ = F.udf(lambda x: 190 if x > 190 \
    else 130 if x < 130 \
    else x, 'int')
weight_local__ = F.udf(lambda x: 110 if x > 100 \
    else 30 if x < 40 \
    else x, 'int')
df = df.withColumn('height_2', height_local__(df['height']))
df = df.withColumn('weight_2', weight_local__(df['weight']))

QI = [
    'birth_year',
    '시도',
    '성별',
    'height_2',
    'weight_2',
    'diag_year',
    'in_year',
    'out_year',
    '흡연상태',
    '음주여부'
]

df1 = df.groupby(*QI).count()
for ii in range(2, 6):
    k = df1.filter(f'count < {ii}').count()
    print(f'\tk-{ii} 불충분: {k}')
    if k == count_all:
        break

dfk = df1.filter('count >= 3').join(df, on=QI, how='left')
dfk.select(*QI, 'count').orderBy('count').show()

dfk.select(*QI).coalesce(1).write.mode('overwrite').csv('C:/Users/JinHwan/data/out-k-3', header=True)
