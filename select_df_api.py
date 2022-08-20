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

# SELECT API
ord.select(ord.order_id, 'order_id', "order_id", (ord.order_id + 100).alias("order10")).show(truncate=False)

# APPLYING FUNCTIONS TO THE COLUMNS
ord.select(ord.order_status, lower(ord.order_status)).show()

# SELECTExpr API
ord.select(substring(ord.order_date, 1, 4).alias("order_year")).show()
ord.selectExpr(" substring(order_date,1,4) as order_year ").show()

# withColumn API
ord.withColumn("order_year", substring(ord.order_date, 1, 4)).show()  # this wil create the new column
ord.withColumn("order_date", substring(ord.order_date, 1, 4)).show()  # this will modify the existing column

# withColumnRename API
ord.withColumnRenamed('order_id', 'OrderId').show()

# Drop
ord_new=ord.drop('order_id', 'order_date')
ord_new.show()

# DROP DUPLICATE
emp.show()
emp.dropDuplicates().show()
emp.dropDuplicates(['name', 'score1', 'score2']).show()
