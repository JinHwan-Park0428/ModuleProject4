import myspark
from myspark import spark as spark
from pyspark.sql import functions as F

df = spark.read.csv('C:/Users/JinHwan/data/fin.csv', header=True)
df = df.withColumn('income_avg', df['income_avg'].cast('int'))

df1 = df.groupBy('income_avg').count()

df1 = df1.orderBy(F.col('count').desc())
df1.show()

df1.limit(1).select('income_avg').show()

print(f'\n\tmode value: {df1.limit(1).collect()[0][0]}')
