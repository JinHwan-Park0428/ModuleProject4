import myspark
from myspark import spark as spark

from pyspark.sql import functions as F
from Cryptodome.Cipher import AES, PKCS1_OAEP
from Cryptodome.Hash import HMAC, SHA256
from Cryptodome.Util.Padding import pad, unpad

KEY = 'key is password..'

df = spark.read.csv('./samples/fin.csv', header=True)

hmac__ = F.udf(lambda x: HMAC.new(KEY.encode('utf8') + x.encode('utf8'), digestmod=SHA256).hexdigest(), 'string')

df = df.withColumn('hmac', hmac__(F.col('이름')))

df.select('hmac', '이름').show(truncate=False)
