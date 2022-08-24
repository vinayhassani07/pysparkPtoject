# IMPORTING
import pyspark.context
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, length

conf = SparkConf().setMaster("local").setAppName("My App")
sc = pyspark.context.SparkContext(conf=conf)

spark = SparkSession.builder.master('local').appName('Column Example').getOrCreate()

print(""" SPARK OBJECT IS CREATED """)
print(""" **************************** """)

# READ CSV
ord = spark.read.load(path='data/orders/part-00000', format='csv', sep=',',
                      schema=('order_id int, order_date string,'
                              ' order_customer_id int,'
                              'order_status string'))

ord.show(truncate=False)

# SPLIT
ord.select(split(ord.order_date, '-').alias("order_date_list")).show(truncate=False)
ord.select(ord.order_date, split(ord.order_date, '-').alias("order_year")).show(truncate=False)

df=spark.createDataFrame([('ab12cd23fe27kl',)], ['s', ])
df.show()
df.select(split(df.s, '[0-9]+')).show()

# LENGTH
ord.withColumn('length_status', length(ord.order_status)).show()
