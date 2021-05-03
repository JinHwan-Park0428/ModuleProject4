import myspark
from myspark import spark as spark

df = spark.read.csv(
        path = './data/test1.csv',
        header = False
        )

df = df.withColumnRenamed('_c0', 'c0')
df = df.withColumnRenamed('_c1', 'c1')
df = df.withColumn('_c2', df['_c2'].cast('int'))
df = df.withColumn('_c3', df['_c3'].cast('double'))
df = df.withColumn('_c4', df['_c4'].cast('double'))
df = df.withColumn('_c5', df['_c5'].cast('double'))
df = df.withColumn('_c7', df['_c7'].cast('double'))

df.printSchema()

df.write.mode('overwrite').parquet('out2')