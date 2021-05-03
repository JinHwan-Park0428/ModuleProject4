# run at terminal
# > python c000-2.py

import myspark
from myspark import spark as spark

df = spark.read.csv('./data/test1.csv')
df.show(3)
print(f'\n\tcount: {df.count()}')

