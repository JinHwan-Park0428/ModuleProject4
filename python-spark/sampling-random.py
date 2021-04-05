import myspark
from myspark import spark as spark

df = spark.read.csv('C:/Users/JinHwan/data/fin.csv', header=True)

df2 = df.sample(0.01)
df2.select('이름', '핸드폰').show(5)
print(f'\n\tdf2 count: {df2.count()}\n')

df3 = df.sample(0.01)
df3.select('이름', '핸드폰').show(5)
print(f'\n\tdf3 count: {df3.count()}\n')