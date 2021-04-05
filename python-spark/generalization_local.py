import myspark
from myspark import spark as spark
from pyspark.sql import functions as F
import math

df = spark.read.csv('C:/Users/JinHwan/data/med.csv', header=True)
df = df.withColumn('체중', df['체중'].cast('int'))

df1 = df.groupby('체중').count()
df1.orderBy('count').show()

weight_10__ = F.udf(lambda x: math.floor(x / 10) * 10, 'int')
df2 = df.withColumn('weight_10', weight_10__(F.col('체중')))

df3 = df2.groupby('weight_10').count()
df3.orderBy('count').show()

weight_local__ = F.udf(lambda x: '110kg 이상' if x > 100 \
    else '30kg 이하' if x < 40 \
    else str(x), 'string')
df4 = df2.withColumn('weight_local', weight_local__(F.col('weight_10')))
df4.groupby('weight_local').count().orderBy('weight_local').show()

weight_local_int__ = F.udf(lambda x: 110 if x > 100 \
    else 30 if x < 40 \
    else x, 'int')

df5 = df2.withColumn('weight_local_int', weight_local_int__(F.col('weight_10')))
df5.groupby('weight_local_int').count().orderBy('weight_local').show()
