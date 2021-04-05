import myspark
from myspark import spark as spark
from pyspark.sql import functions as F
import random
from datetime import datetime
from datetime import timedelta

df = spark.read.csv('C:/Users/JinHwan/data/med.csv', header=True)
df = df.withColumn('noise', F.round(F.rand() * 6).cast('int') - 3)


@F.udf('date')
def date_add__(x, y):
    return datetime.strptime(x, '%Y-%m-%d') + timedelta(days=y)


df = df.withColumn('date_1', date_add__(df['진단일자'], df.noise))
df = df.withColumn('date_2', date_add__(df['입원일자'], df.noise))
df = df.withColumn('date_3', date_add__(df['퇴원일자'], df.noise))
df.select('noise', 'date_1', '진단일자', 'date_2', '입원일자', 'date_3', '퇴원일자').show()
