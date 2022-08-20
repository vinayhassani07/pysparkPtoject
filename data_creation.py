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
                      schema=('order_id int, order_date string,'
                              ' order_customer_id int,'
                              'order_status string'))

ord.show(truncate=False)

data = [('Robert', 35, 40, 40), ('Robert', 35, 40, 40), ('Ram', 31, 33, 29), ('Ram', 31, 33, 91)]
emp=spark.createDataFrame(data=data, schema=['name', 'score1', 'score2', 'score3'])
emp.show()
