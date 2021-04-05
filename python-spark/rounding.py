import myspark
from myspark import spark as spark
from pyspark.sql import functions as F
import math

round__ = F.udf(lambda x, step: round(x, step))
ceil__ = F.udf(lambda x, step: step * math.ceil(x / step))
floor__ = F.udf(lambda x, step: step * math.floor(x / step))
trunc__ = F.udf(lambda x, step: step * math.trunc(x /step))

df = spark.read.csv('C:/Users/JinHwan/data/med.csv', header=True)
df = df.withColumn('신장', df['신장'].cast('int'))

df = df.withColumn('h_round', round__(F.col('신장'), F.lit(-1)))
df = df.withColumn('h_ceil', ceil__(F.col('신장'), F.lit(10)))
df = df.withColumn('h_floor', floor__(F.col('신장'), F.lit(10)))
df = df.withColumn('h_trunc', floor__(F.col('신장'), F.lit(10)))

df.select('신장', 'h_round', 'h_ceil', 'h_floor', 'h_trunc').show()