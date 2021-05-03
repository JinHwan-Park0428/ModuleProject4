import myspark
from myspark import spark as spark

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

df = spark.read.csv('./data/fin.csv', header=True)

df = df.withColumn('income_avg', df['income_avg'].cast('int'))

print('\n')

df.select(
    F.lit(df.count()).alias('count1'),
    F.count('income_avg').alias('count2'),
    F.min('income_avg').alias('min'),
    F.max('income_avg').alias('max'),
    F.mean('income_avg').alias('mean'),
    F.sum('income_avg').cast(DecimalType(38, 0)).alias('sum')
    ).show(vertical=True)
