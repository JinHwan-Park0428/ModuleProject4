import myspark
from myspark import spark as spark
from pyspark.sql import functions as F
import uuid

df = spark.read.csv('C:/Users/JinHwan/data/fin.csv', header=True)

df = df.withColumn('idx', F.lit(str(uuid.uuid4())))

df.select('idx', '일련변호').show(truncate=False)