#  Create a spark Object
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)

spark = SparkSession \
    .builder \
    .master('local') \
    .appName("Python dataFrame Basic Example .") \
    .getOrCreate()

print("Spark Object is Created .....")
print("****** Spark Conf properties is ****** ")
print(spark.conf.get('spark.sql.shuffle.partitions'))
print("****** Spark Conf properties  after Changing ****** ")
print(spark.conf.set('spark.sql.shuffle.partitions', 250))

spark.stop()
