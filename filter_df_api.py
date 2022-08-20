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

data = [('Robert', 35, 40, 40), ('Robert', 35, 40, 40), ('Ram', 31, 33, 29), ('Ram', 31, 33, 91)]
emp = spark.createDataFrame(data=data, schema=['name', 'score1', 'score2', 'score3'])
emp.show()

# Where API
ord.where((ord.order_id > 10) & (ord.order_id < 20)).show(truncate=False)

# USING isin()
ord.where(ord.order_status.isin('COMPLETE', 'CLOSED')).show(truncate=False)

# USING SQL OPERATOR
ord.where("order_status in ('COMPLETE', 'CLOSED')").show(truncate=False)
