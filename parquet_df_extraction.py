# PREPARING THE DATA
import pyspark.context
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws

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

order.write.save('C:/Users/ROG/PycharmProjects/pysparkPtoject/data/orderParquet', mode='overwrite', format='parquet',
                 partitionBy='order_status')

print(spark.conf.get("spark.sql.parquet.compression.codec"))

# NOTE : SAME WILL BE FOR ORC FILE
order.write.save('C:/Users/ROG/PycharmProjects/pysparkPtoject/data/orderAVRO', format='avro',
                 partitionBy='order_status')

