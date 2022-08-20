# PREPARING THE DATA
import pyspark.context
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, substring

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

data = [('a', 1), ('d', 4), ('c', 3), ('b', 2), ('e', 5)]
df = spark.createDataFrame(data=data, schema='col1 string, col2 string')
df.show()

# SORTING
ord.sort(ord.order_customer_id.asc()).show()

ord.sort(ord.order_date.desc(), ord.order_customer_id.asc()).show()

ord.sort(ord.order_date, ord.order_customer_id.asc(), ascending=[0, 1]).show()

# USING SORTWITHINPARTITION
df.rdd.getNumPartitions()
df.rdd.glom().collect()
df.sortWithinPartitions(df.col1.asc(), df.col2.asc()).show()


