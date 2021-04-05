import myspark
from myspark import spark as spark
from pyspark.sql import functions as F
from Cryptodome.Hash import SHA256

@F.udf('string')
def hash(salt, msg):
    h = SHA256.new()
    h.update(salt.encode('utf-8'))
    h.update(msg.encode('utf-8'))
    return h.hexdigest()

SALT = 'Salt is salty..'

df = spark.read.csv('C:/Users/JinHwan/data/fin.csv', header=True)

df = df.withColumn('hash', hash(F.lit(SALT), F.col('이름')))

df.select('hash', '이름').show(truncate=False)