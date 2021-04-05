import myspark
from myspark import spark as spark
from pyspark.sql import functions as F
from Cryptodome.Hash import SHA256
from datetime import datetime

# 제거할 컬럼 = [핸드폰, 주소(읍면동), 이름]
# 키 컬럼 = [핸드폰, 성별, 생년월일]
DATA_ROOT = 'C:/Users/JinHwan/Desktop/SK_Infosec_Academy/worked_files/MDproject4'
FILE_ROOT = 'C:/Users/JinHwan/Desktop/SK_Infosec_Academy/worked_files/MDproject4/금융_40만-210401.csv'
SALT = b'Salt is salty..'
KEY_COLUMNS = ['핸드폰', '성별', '생년월일']


@F.udf('string')
def hash(msg):
    h = SHA256.new(SALT)
    h.update(msg.encode('utf-8'))
    return h.hexdigest()

def make_key_file():
    df = spark.read.csv(f'{FILE_ROOT}', header=True)
    df = df.withColumn('join_key', hash(F.concat_ws('|', *KEY_COLUMNS)))
    df = df.withColumn('row_id', F.monotonically_increasing_id())
    df.write.mode('overwrite').csv(f'{DATA_ROOT}/join/connect.data', header=True)

    dfkey = df.select('join_key', 'row_id')
    dfkey.write.mode('overwrite').csv(f'{DATA_ROOT}/join/connect.key', header=True)

    dfdata = df.drop('join_key', *KEY_COLUMNS + ['읍면동', '이름'])
    dfdata.write.mode('overwrite').csv(f'{DATA_ROOT}/join/connect.data', header=True)

    df = spark.read.csv(f'{DATA_ROOT}/join/connect.key', header=True).show(15, False)
    df = spark.read.csv(f'{DATA_ROOT}/join/connect.data', header=True).show(15, False)


if __name__ == '__main__':
    make_key_file()
