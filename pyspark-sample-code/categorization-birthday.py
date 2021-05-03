import myspark
from myspark import spark as spark

from pyspark.sql import functions as F
import math

round__ = F.udf(lambda x, step: round(x, step))
ceil__ = F.udf(lambda x, step: step * math.ceil(x / step))
floor__ = F.udf(lambda x, step: step * math.floor(x / step))
trunc__ = F.udf(lambda x, step: step * math.trunc(x / step))

df = spark.read.csv('./data/med.csv', header=True)

# categorization
born_year__ = F.udf(lambda x: x[:6], 'string')
df = df.withColumn('born_year', born_year__(F.col('생년월일')))
# df.select('born_year').show()

df.groupby('born_year').count().orderBy('count').show()

