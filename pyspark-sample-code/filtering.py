import myspark
from myspark import spark as spark

from pyspark.sql import functions as F

df = spark.read.csv('./data/med.csv', header=True)

df = df.withColumn('신장', df['신장'].cast('int'))

# check
df1 = df.groupby('신장').count()
df1.orderBy('신장').show()
df1.orderBy(F.col('신장').desc()).show()

# filter
df2 = df.filter('`신장` >= 100 and `신장` <= 200')
df2.select(F.min('신장'), F.max('신장')).show()
