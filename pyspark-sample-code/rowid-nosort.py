import myspark
from myspark import spark as spark

from pyspark.sql.window import Window as W
from pyspark.sql import functions as F

df = spark.read.csv('./data/fin.csv', header=True)

# not consecutive. start 0
df = df.withColumn('idx', F.monotonically_increasing_id())

# consecutive. start 1
# window = W.orderBy('idx')
df = df.withColumn('idx2', F.row_number().over(W.orderBy('idx')))

df.select('idx', 'idx2', '일련번호').show()
df.select(F.min('idx'), F.max('idx'), F.min('idx2'), F.max('idx2')).show(vertical=True)