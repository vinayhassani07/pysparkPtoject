# IMPORTING
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sumDistinct, count, countDistinct, first, last, collect_list, collect_set

spark = SparkSession.builder.master('local').appName('SET EXAMPLE ').getOrCreate()

print(""" SPARK OBJECT IS CREATED """)
print(""" **************************** """)

# READ CSV
orditem = spark.read.load(path='data/order_items/part-00000', format='csv', sep=',',
                          schema=('order_item_id int, order_item_order_id int,'
                                  ' order_item_product_id int,'
                                  'quantity int, subtotal decimal(5,2), price decimal(5,2)'))

orditem.show(truncate=False)

# SUMMARY FUNCTION
orditem.summary().show()
orditem.select(orditem.price).summary().show()

# AVERAGE
orditem.select(avg(orditem.price)).show()

# MAX AND MIN
orditem.select(round(avg(orditem.price), 2).alias("avg_price"), max(orditem.price), min(orditem.price)).show()

# SUM AND SUM DISTINCT
orditem.select(sum(orditem.price), sumDistinct(orditem.price)).show()

# COUNT AND COUNT DISTINCT
orditem.select(count(orditem.price), countDistinct(orditem.price)).show()

# FIRST AND LAST
orditem.sort(orditem.price.asc()).select(first(orditem.price), last(orditem.price)).show()

# COLLECT SET AND COLLECT LIST
df =spark.createDataFrame(data=[(1, 100), (2, 150), (3, 200), (4, 50), (5, 50)], schema='id int, salary int')
df.show()
df.select(collect_list(df.salary).alias('list'), collect_set(df.salary).alias('set')).show(truncate=False)
