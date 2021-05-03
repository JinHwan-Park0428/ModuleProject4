# 수행처: 결합전문기관
# 결합 후
#   joined data 제공 -> 데이터수신사

import myspark
from myspark import spark as spark

DATA_ROOT = './data'

ROW_ID_COLUMN = 'rowid'

customers = ['fin', 'med']

def read_csv(path, customer):
    df = spark.read.csv(path, header=True)
    new_names = [f'{customer}_{colname}' for colname in df.columns]
    df = df.toDF(*new_names)
    return df

df = spark.read.csv(f'{DATA_ROOT}/join/joined.key', header=True)

counts = {}
for customer in customers:
    dfdata = read_csv(f'{DATA_ROOT}/join/{customer}.data', customer)
    counts[customer] = dfdata.count()
    df = df.join(dfdata, on=[f'{customer}_{ROW_ID_COLUMN}'], how='left')

df = df.drop(*[f'{customer}_{ROW_ID_COLUMN}' for customer in customers])
df.write.mode('overwrite').csv(f'{DATA_ROOT}/join/joined.data', header=True)

count_joined = df.count()
for customer in customers:
    print(f'\t{customer}: {count_joined} / {counts[customer]}: {count_joined / counts[customer]}')
