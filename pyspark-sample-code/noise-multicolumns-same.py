import myspark
from myspark import spark as spark

from pyspark.sql import functions as F
from datetime import datetime
from datetime import timedelta

df = spark.read.csv('./data/med.csv', header=True)

# add noise column
df = df.withColumn('noise', F.round(F.rand()*6).cast('int') - 3)
# df.select('noise').show()

# noise
@F.udf('date')
def date_add__(x, y):
    date = datetime.strptime(x, '%Y-%m-%d')
    return date + timedelta(days=y)

df = df.withColumn('date_1', date_add__(df['진단일자'], df.noise))
df = df.withColumn('date_2', date_add__(df['입원일자'], df.noise))
df = df.withColumn('date_3', date_add__(df['퇴원일자'], df.noise))
df.select('noise', 'date_1', '진단일자', 'date_2', '입원일자', 'date_3', '퇴원일자').show()
