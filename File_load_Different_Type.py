#  Create a spark Object
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.1.2'

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)

spark = SparkSession \
    .builder \
    .master('local') \
    .appName("Python dataFrame Basic Example .") \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", 5)
print("Spark Object is Created .....")
# READ CSV
# df = spark.read.load(path='data/orders/part-00000', format='csv', sep=',',
#                      schema=('order_id int, order_date string,'
#                              ' order_customer_id int,'
#                              'order_status string'))
# df = spark.read.load(path='data/orders/part-00000', format='csv', sep=',',
#                      inferSchema=True, header=False)
# READ TEXT
# df=spark.read.load(path='data/orders/part-00000', format='text')

# CONVERTING THE FILE TO ORC AND PARQUET
#  df = spark.read.load(path='data/orders/part-00000', format='csv', sep=',')
#  df.write.orc('data/orders_orc/part-00000')
#  df.write.parquet('data/orders_parquet/part-00000')

# READ ORC
# df_orc=spark.read.load(path='data/orders_orc/part-00000', format='orc')
# df_par=spark.read.load(path='data/orders_parquet/part-00000', format='parquet')
# print(" *** ORC SCHEMA *** ")
# df_orc.printSchema()
# print(" *** PAR SCHEMA *** ")
# df_par.printSchema()
# print(" *** ORC RECORD *** ")
# df_orc.show(10, truncate=False)
# print(" *** PAR RECORD *** ")
# df_par.show(10, truncate=False)

# READ AVRO
# df_avro=spark.read.load(path='data/orders/part-00000', format='avro')
#
# print("*** SCHEMA ***")
# df_avro.printSchema()
#
# print("***  DATA ***")
# df_avro.show(10, truncate=False)

spark.stop()
