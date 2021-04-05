import myspark
from myspark import spark as spark

DATA_ROOT = 'C:/Users/JinHwan/Desktop/SK_Infosec_Academy/worked_files/MDproject4'

ROW_ID_COLUMN = 'rowid'

customers = ['A', 'B']


def read_csv(path, customer):
    if customer == 'A':
        df = spark.read.csv(path, header=True, encoding='euc-kr')
    else:
        df = spark.read.csv(path, header=True)
    new_names = [f'{customer}_{colname}' for colname in df.columns]
    df = df.toDF(*new_names)
    return df


df = spark.read.csv(f'{DATA_ROOT}/join/5&6joined.key', header=True)

counts = {}

for customer in customers:
    dfdata = read_csv(f'{DATA_ROOT}/join/{customer}.data', customer)
    counts[customer] = dfdata.count()
    df = df.join(dfdata, on=[f'{customer}_{ROW_ID_COLUMN}'], how='left')

df = df.drop(*[f'{customer}_{ROW_ID_COLUMN}' for customer in customers])
df.coalesce(1).write.mode('overwrite').csv(f'{DATA_ROOT}/join/5&6joined.data', header=True)

count_joined = df.count()
for customer in customers:
    print(f'\t{customer}: {count_joined} / {counts[customer]}: {count_joined / counts[customer]}')

df = spark.read.csv(f'{DATA_ROOT}/join/5&6joined.data', header=True).show()
