# PREPARING THE DATA
import pyspark.context
from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf().setMaster("local").setAppName("My App")
sc = pyspark.context.SparkContext(conf=conf)

spark = SparkSession.builder.master('local').appName('Column Example').getOrCreate()

print(""" SPARK OBJECT IS CREATED """)
print(""" **************************** """)

# READ CSV
order = spark.read.load(path='data/orders/part-00000', format='csv', sep=',',
                        schema=('order_id int, order_date timestamp,'
                                ' order_customer_id int,'
                                'order_status string'))
order.show(truncate=False)
order.printSchema()

order.write.saveAsTable(name='db2.order_test', format='orc')
