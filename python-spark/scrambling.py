import myspark
from myspark import spark as spark
from pyspark.sql import functions as F
import random


@F.udf('string')
def shuffle(x):
    x2 = list(x.replace('-', '')[3:])
    random.shuffle(x2)
    x2.insert(-4, '-')
    return x[:4] + ''.join(x2)


df = spark.read.csv('C:/Users/JinHwan/data/fin.csv', header=True)

df = df.withColumn('shuffle', shuffle(F.col('핸드폰')))

df.select('shuffle', '핸드폰').show(truncate=False)
