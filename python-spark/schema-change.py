import myspark
from myspark import spark as spark

df = spark.read.csv(
    path='C:/Users/JinHwan/data/test1.csv',
    header=False
)

df = df.withColumnRenamed('_c0', 'c0')
df = df.withColumnRenamed('_c1', 'c1')
# df = df.withColumnRenamed('_c2', 'c2').withColumn('c2', df['c2'].cast('int'))
# df = df.withColumnRenamed('_c3', 'c3').withColumn('c3', df['c3'].cast('double'))
# df = df.withColumnRenamed('_c4', 'c4').withColumn('c4', df['c4'].cast('double'))
# df = df.withColumnRenamed('_c5', 'c5').withColumn('c5', df['c5'].cast('double'))
df = df.withColumnRenamed('_c2', 'c2')
df = df.withColumnRenamed('_c3', 'c3')
df = df.withColumnRenamed('_c4', 'c4')
df = df.withColumnRenamed('_c5', 'c5')
df = df.withColumnRenamed('_c6', 'c6')
# df = df.withColumnRenamed('_c7', 'c7').withColumn('c7', df['c7'].cast('double'))
df = df.withColumnRenamed('_c7', 'c7').withColumn('c7', df['c7'].cast('double'))

df.printSchema()

df.write.mode('overwrite').parquet('out2')