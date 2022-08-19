from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Column
from pyspark.sql.functions import col

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.master('local').appName('Column Example').getOrCreate()

print(""" SPARK OBJECT IS CREATED """)
print(""" **************************** """)

# READ CSV
ord = spark.read.load(path='data/orders/part-00000', format='csv', sep=',',
                      schema=('order_id int, order_date string,'
                              ' order_customer_id int,'
                              'order_status string'))
# SELECT ING A COLUMN
ord.select(ord.order_id).show()
ord.select(ord["order_id"]).show()
ord.select(col("*")).show()
ord.select(col("order_id")).show()

# GIVING ALIAS NAME TO COLUMN
ord.select(ord.order_id.alias("orderId")).show()

# ORDER A COLUMN
ord.select(ord.order_status).show()
ord.orderBy(ord.order_status.asc()).select(ord.order_status).show()
ord.orderBy(ord.order_status.asc()).select(ord.order_status).distinct().show()

# CASTING TO OTHER DATA TYPE
ord.printSchema()
print(ord.dtypes)
ord_new = ord.select(ord.order_id.cast("String"))
print(ord_new.dtypes)

# Between
ord.select(ord.order_id.between(10, 20)).show()
ord.where(ord.order_id.between(10, 20)).show()  # instead of select we use where for this type of condition
