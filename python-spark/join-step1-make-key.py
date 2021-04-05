import myspark
from myspark import spark as spark
from pyspark.sql import functions as F
from Cryptodome.Hash import SHA256

DATA_ROOT = 'C:/Users/JinHwan/data'
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
    dfkey.write.mode('overwrite').csv(f'{DATA_ROOT}/join/{customer}.key', header=True)

    dfdata = df.drop(JOIN_KEY_COLUMN, *KEY_COLUMNS)
    dfdata.write.mode('overwrite').csv(f'{DATA_ROOT}/join/{customer}.data', header=True)

    df = spark.read.csv('C:/Users/JinHwan/data/join/fin.key', header=True).show(15, False)
    df = spark.read.csv('C:/Users/JinHwan/data/join/fin.data', header=True).select('rowid', '핸드폰', '생년월일',
                                                                                   '성별').show(15, False)


for customer in customers:
    make_key_file(customer)
