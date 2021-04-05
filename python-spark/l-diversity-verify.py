import myspark
from myspark import spark as spark
from pyspark.sql import functions as F

QI = [
    'birth_year',
    '시도',
    '성별',
    'height_2',
    'weight_2'
]

df = spark.read.csv('C:/Users/JinHwan/data/out-k3-l2/*.csv', header=True)
df1 = df.select(*QI, '진료과명').distinct().groupby(*QI).count()
df1 = df1.withColumnRenamed('count', 'cnt')
df2 = df1.groupby('cnt').count().orderBy('cnt').show()