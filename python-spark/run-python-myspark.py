import myspark
from myspark import spark as spark

df = spark.read.csv('C:/Users/JinHwan/data/test1.csv')
df.show(3)
print(df.count())
