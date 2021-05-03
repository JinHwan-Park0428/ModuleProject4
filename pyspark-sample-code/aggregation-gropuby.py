import myspark
from myspark import spark as spark

from pyspark.sql import functions as F

df = spark.read.csv('./data/fin.csv', header=True)

df = df.withColumn('income_avg', df['income_avg'].cast('int'))
df.select('고객등급', 'income_avg').show(truncate=False)

df1 = df.groupby('고객등급').agg(F.mean('income_avg').alias('mean'))
df1.show()

df2 = df.join(df1, on=['고객등급'], how='left')

df2 = df2.drop('income_avg')
df2 = df2.withColumnRenamed('mean', 'income_avg')

df2.select('고객등급', 'income_avg').show(truncate=False)
