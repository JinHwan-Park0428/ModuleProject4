import myspark
from myspark import spark as spark
from pyspark.sql.window import Window as W
from pyspark.sql import functions as F

df = spark.read.csv('C:/Users/JinHwan/data/fin.csv', header=True)

window = W.orderBy(F.col('일련번호'))
df = df.withColumn('idx', F.row_number().over(window))

df.select('idx', '일련번호').show()
