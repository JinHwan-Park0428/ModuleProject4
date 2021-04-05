import myspark
from myspark import spark as spark
from pyspark.sql import functions as F
import random
from datetime import datetime
from datetime import timedelta


@F.udf('date')
def add_date__(x):
    date = datetime.strptime(x, '%Y-%m-%d')
    return date + timedelta(days=random.randrange(-3, 3))


df = spark.read.csv('C:/Users/JinHwan/data/med.csv', header=True)
df = df.withColumn('진단일자', df['진단일자'].cast('date'))
df = df.withColumn('date_1', F.date_add(df['진단일자'], random.randrange(-3, 3)))
df = df.withColumn('date_2', add_date__(df['진단일자']))
df.select('진단일자', 'date_1').show()
df.select('진단일자', 'date_2').show()
