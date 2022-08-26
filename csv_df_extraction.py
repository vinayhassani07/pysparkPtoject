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
ord = spark.read.load(path='data/orders/part-00000', format='csv', sep=',',
                      schema=('order_id int, order_date timestamp,'
                              ' order_customer_id int,'
                              'order_status string'))
ord.show(truncate=False)

ordCountByStatus = ord.groupBy(ord.order_status).count()
ordCountByStatus.show(truncate=False)

print(" NO OF PARTITION IS  " + str(ordCountByStatus.rdd.getNumPartitions()))

ordCountByStatus.coalesce(2).write.save('C:/Users/ROG/PycharmProjects/pysparkPtoject/data/ordCountByStatus',
                                        format='csv', sep='#', mode='overwrite', header=True)
