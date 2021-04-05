import myspark
from myspark import spark as spark
from pyspark.sql import functions as F

df = spark.read.csv('C:/Users/JinHwan/data/fin.csv', header=True)
df = df.withColumn('income_avg', df['income_avg'].cast('int'))

df.select(
    F.percentile_approx('income_avg', 0.5, 1).alias('accuracy'),
    F.percentile_approx('income_avg', 0.5, 100).alias('accuracy100'),
    F.percentile_approx('income_avg', 0.5, 10000).alias('accuracy_default'),
    F.percentile_approx('income_avg', [0.25, 0.5, 0.75]).alias('accuracy_quarter')
).show(truncate=False)
