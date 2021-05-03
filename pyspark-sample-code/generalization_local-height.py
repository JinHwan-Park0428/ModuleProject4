import myspark
from myspark import spark as spark

from pyspark.sql import functions as F
import math

df = spark.read.csv('./data/med.csv', header=True)
df = df.withColumn('신장', df['신장'].cast('int'))

# check 1cm count
df1 = df.groupby('신장').count()
df1.orderBy('count').show()

# categorization 10cm
cat_10__ = F.udf(lambda x: math.floor(x / 10) * 10, 'int')
df2 = df.withColumn('height_10', cat_10__(df['신장']))
df2.select('height_10', '신장').show()

# check 10kg count
df3 = df2.groupby('height_10').count()
df3.orderBy('count').show()

# local generalization: int
height_local__ = F.udf(lambda x: 190 if x > 190 else 130 if x < 130 else x, 'int')
df5 = df2.withColumn('height_local', height_local__(F.col('height_10')))
df5.groupby('height_local').count().orderBy('height_local').show()