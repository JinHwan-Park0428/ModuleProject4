import myspark
from myspark import spark as spark

DATA_ROOT = 'C:/Users/JinHwan/data'

JOIN_KEY_COLUMN = 'joinkey'
ROW_ID_COLUMN = 'rowid'

customers = ['fin', 'med']


def read_csv(path, customer):
    df = spark.read.csv(path, header=True)
    df = df.withColumnRenamed(ROW_ID_COLUMN, f'{customer}_{ROW_ID_COLUMN}')
    return df


counts = {}

df = read_csv(f'{DATA_ROOT}/join/{customers[0]}.key', customers[0])
counts[customers[0]] = df.count()

for customer in customers[1:]:
    dfnext = read_csv(f'{DATA_ROOT}/join/{customer}.key', customer)
    counts[customer] = dfnext.count()
    df = df.join(dfnext, on=[JOIN_KEY_COLUMN], how='inner')

df = df.select(*[f'{customer}_{ROW_ID_COLUMN}' for customer in customers])
df.write.mode('overwrite').csv(f'{DATA_ROOT}/join/joined.key', header=True)

count_joined = df.count()
for customer in customers:
    print(f'\t{customer}: {count_joined} / {counts[customer]}: {count_joined / counts[customer]}')

df = spark.read.csv('C:/Users/JinHwan/data/join/joined.key', header=True).show(15, False)
