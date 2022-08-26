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

orderText= order.select(concat_ws('\t', order.order_id, order.order_date, order.order_customer_id, order.order_status).
                        alias("col1"))
orderText.show(truncate=False)
orderText.printSchema()

orderText.repartition(10).write.save('C:/Users/ROG/PycharmProjects/pysparkPtoject/data/orderText', format='text')
