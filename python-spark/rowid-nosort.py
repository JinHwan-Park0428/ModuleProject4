import myspark
from myspark import spark as spark
from pyspark.sql.window import Window as W
from pyspark.sql import functions as F

df = spark.read.csv('C:/Users/JinHwan/data/fin.csv', header=True)

df = df.withColumn('idx', F.monotonically_increasing_id())

df = df.withColumn('idx', F.row_number().over(W.orderBy('idx')))
df.select(F.min('idx'), F.max('idx'), F.min('idx2'), F.max('idx2')).show()
