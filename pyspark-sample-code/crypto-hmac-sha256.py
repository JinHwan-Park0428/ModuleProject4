import myspark
from myspark import spark as spark

from pyspark.sql import functions as F

# pip install pycryptodomex
from Cryptodome.Hash import HMAC, SHA256


@F.udf('string')
def hmac(key, msg):
    h = HMAC.new(key.encode('utf-8'), digestmod=SHA256)
    h.update(msg.encode('utf-8'))
    return h.hexdigest()


KEY = 'key is password..'

df = spark.read.csv('./data/fin.csv', header=True)

df = df.withColumn('hmac', hmac(F.lit(KEY), F.col('이름')))

df.select('hmac', '이름').show(truncate=False)
