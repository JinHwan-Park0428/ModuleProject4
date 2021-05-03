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
