import myspark
from myspark import spark as spark

from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, DoubleType

customSchema = StructType([
    StructField('c0', StringType(), False),
    StructField('c1', StringType(), False),
    StructField('c2', IntegerType(), False),
    StructField('c3', DoubleType(), False),
    StructField('c4', DoubleType(), False),
    StructField('c5', DoubleType(), False),
    StructField('c6', StringType(), False),
    StructField('c7', DoubleType(), False)
    ])
    
df = spark.read.csv(
        path = './data/test1.csv',
        schema = customSchema,
        header = False
        )
        
df.printSchema()
