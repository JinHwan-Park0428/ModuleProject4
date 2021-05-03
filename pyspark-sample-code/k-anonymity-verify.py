import myspark
from myspark import spark as spark

from pyspark.sql import functions as F

QI = ['birth_year', '시도', '성별', 'height_2', 'weight_2', 'diag_year', 'in_year', 'out_year', '흡연상태', '음주여부']

df = spark.read.csv('./data/out-k-3/*.csv', header=True)
df.groupby(*QI).count().select(F.min('count')).show()
