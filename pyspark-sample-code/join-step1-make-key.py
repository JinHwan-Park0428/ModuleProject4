# 수행처: 데이터보유사
# 키 생성 방법 협의: 결합키관리기관
# 생성 후
#   rowid, joinkey 제공 -> 결합키관리기관
#   rowid, data 제공 -> 결합전문기관
# 데이터보유사가 자기 것만 하겠지만
#   이 샘플에서는 2개 보유사 것을 만든다
#   med사, fin사

import myspark
from myspark import spark as spark
from pyspark.sql import functions as F

# pip install pycryptodomex
from Cryptodome.Hash import SHA256

DATA_ROOT = './data'

JOIN_KEY_COLUMN = 'joinkey'
ROW_ID_COLUMN = 'rowid'
SALT = b'Salt is salty..'
KEY_COLUMNS = ['성별', '생년월일', '핸드폰']

customers = ['fin', 'med']

@F.udf('string')
def hash(msg):
    h = SHA256.new(SALT)
    h.update(msg.encode('utf-8'))
    return h.hexdigest()

def make_key_file(customer):
    df = spark.read.csv(f'{DATA_ROOT}/{customer}.csv', header=True)
    df = df.withColumn(JOIN_KEY_COLUMN, hash(F.concat_ws('|', *KEY_COLUMNS)))
    df = df.withColumn(ROW_ID_COLUMN, F.monotonically_increasing_id())
    df.write.mode('overwrite').csv(f'{DATA_ROOT}/join/{customer}.data', header=True)

    dfkey = df.select(JOIN_KEY_COLUMN, ROW_ID_COLUMN)
    dfkey.coalesce(1).write.mode('overwrite').csv(f'{DATA_ROOT}/join/{customer}.key', header=True)

    dfdata = df.drop(JOIN_KEY_COLUMN, *KEY_COLUMNS)
    dfdata.coalesce(1).write.mode('overwrite').csv(f'{DATA_ROOT}/join/{customer}.data', header=True)

for customer in customers:
    make_key_file(customer)
