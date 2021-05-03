import myspark
from myspark import spark as spark

from pyspark.sql import functions as F
import uuid

df = spark.read.csv('./data/fin.csv', header=True)

df = df.withColumn('idx', F.lit(str(uuid.uuid4())))

df.select('idx', '일련번호').show(truncate=False)
