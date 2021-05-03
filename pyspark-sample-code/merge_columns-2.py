import myspark
from myspark import spark as spark

from pyspark.sql import functions as F

@F.udf('string')
def addr(x,y,z):
    return f'{x} {y} {z}'

df = spark.read.csv('./data/med.csv', header=True)

# 1
addr__ = F.udf(lambda x, y, z: f'{x} {y} {z}', 'string')
df = df.withColumn('address_1', addr__(F.col('시도'), F.col('시군구'), F.col('읍면동')))
# df.select('address').show()

# 2
df = df.withColumn('address_2', F.concat_ws(' ', *['시도', '시군구', '읍면동']))
df.select('address_2').show()

# df = df.coalesce(1).sortWithinPartitions(['환자ID'])
# df.write.mode('overwrite').csv('./data/med1', header=True, emptyValue='')
