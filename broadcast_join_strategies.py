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

print(" **** PROPERTIES *** " + str(spark.conf.get("spark.sql.autoBroadcastJoinThreshold")))
# READ CSV
order = spark.read.load(path='data/orders/part-00000', format='csv', sep=',',
                        schema=('order_id int, order_date timestamp,'
                                ' order_customer_id int,'
                                'order_status string'))
order.show(truncate=False)
order.printSchema()

order_item = spark.read.load(path='data/order_items/part-00000', format='csv', sep=',',
                             schema=('order_item_id int, order_item_order_id int,'
                                     ' order_item_product_id int,'
                                     'quantity int, subtotal decimal(5,2), price decimal(5,2)'))

order_item.show(truncate=False)

joined = order.join(order_item, order.order_id == order_item.order_item_id)
joined.show(truncate=False)
joined.explain()

LargeDF= spark.range(1, 1000000000)
data= [(1, 'a'), (2, 'b'), (3, 'c')]
schema= ['id', 'col2']
smallDF= spark.createDataFrame(data, schema)
joined2 =LargeDF.join(smallDF, "id")
joined2.explain()

joined3= smallDF.hint("BROADCAST").join(LargeDF, "id")
joined3.explain()
