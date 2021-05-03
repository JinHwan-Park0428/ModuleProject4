import myspark
from myspark import spark as spark

from pyspark.sql import functions as F

@F.udf('string')
def addr(x,y,z):
    return f'{x} {y} {z}'

df = spark.read.csv('./data/med.csv', header=True)
df = df.withColumn('address', addr(F.col('시도'), F.col('시군구'), F.col('읍면동')))
# df.select('address').show()

df = df.coalesce(1).sortWithinPartitions(['환자ID'])
# df.write.mode('overwrite').csv('./data/med1', header=True, emptyValue='')
