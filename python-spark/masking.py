import myspark
from myspark import spark as spark
from pyspark.sql import functions as F
import re


@F.udf('string')
def mask_1(x):
    return x[:1] + 'XX'


@F.udf('string')
def mask_2(x):
    return re.sub(r'-(\d{3,4})-', r'-xxxx-', x)


df = spark.read.csv('C:/Users/JinHwan/data/fin.csv', header=True)

df = df.withColumn('mask1', mask_1(F.col('이름')))
df = df.withColumn('mask2', mask_2(F.col('핸드폰')))

df.select('mask1', '이름', 'mask2', '핸드폰').show(truncate=False)
