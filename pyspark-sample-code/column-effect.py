import myspark
from myspark import spark as spark

from pyspark.sql import functions as F
import math

df = spark.read.csv('./data/med.csv', header=True)
# count_all = df.count()

# categorization
year__ = F.udf(lambda x: x[:4], 'string')
df = df.withColumn('birth_year', year__(df['생년월일']))
df = df.withColumn('diag_year', year__(df['진단일자']))
df = df.withColumn('in_year', year__(df['입원일자']))
df = df.withColumn('out_year', year__(df['퇴원일자']))

# categorization
df = df.withColumn('신장', df['신장'].cast('int'))
df = df.withColumn('체중', df['신장'].cast('int'))
cat_10__ = F.udf(lambda x: math.floor(x / 10) * 10, 'int')
df = df.withColumn('height', cat_10__(df['신장']))
df = df.withColumn('weight', cat_10__(df['체중']))

# local generalization
height_local__ = F.udf(lambda x: 190 if x > 190 else 130 if x < 130 else x, 'int')
weight_local__ = F.udf(lambda x: 110 if x > 100 else 30 if x < 40 else x, 'int')
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


dfc = df.groupby(*QI).count()
count_all = dfc.filter('count = 1').count()
print(f'count_all: {count_all}')

counts = {}
for ii in range(0, len(QI)):
    qi = QI[:ii] + QI[ii+1:]
    dfc = df.groupby(*qi).count()
    counts[QI[ii]] = dfc.filter('count = 1').count()

for qi in QI:
    print(f'{qi} : {counts[qi]} / {count_all} = {round((1 - counts[qi] / count_all) * 100, 2)} %')
