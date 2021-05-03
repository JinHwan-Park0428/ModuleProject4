# run at terminal
# > python c000-1.py

# pip install findspark
import findspark
findspark.init()

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

spark = SparkSession.builder \
    .master('local') \
    .appName('timecandy') \
    .config('spark.driver.memory', '12g') \
    .getOrCreate()

df = spark.read.csv('./data/test1.csv')
df.show(3)
df.count()
