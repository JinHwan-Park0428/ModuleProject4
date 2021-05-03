import myspark
from myspark import spark as spark

from pyspark.sql.window import Window as W
from pyspark.sql import functions as F

df = spark.read.csv('./data/fin.csv', header=True)

# consecutive. start 1
window = W.orderBy(F.col('일련번호'))
df = df.withColumn('idx', F.row_number().over(window))

df.select('idx', '일련번호').show()
