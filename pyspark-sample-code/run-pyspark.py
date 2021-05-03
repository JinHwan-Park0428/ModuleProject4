# run at pyspark console
# > pyspark
# >>> exec(open('run_1_pyspark.py').read())

df = spark.read.csv('./data/test1.csv')
df.show(3)
df.count()
