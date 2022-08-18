#  Create a spark Object
import pandas as pd
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)

spark = SparkSession \
    .builder \
    .master('local') \
    .appName("Python dataFrame Basic Example .") \
    .getOrCreate()

print("Spark Object is Created .....")

# lst=[('robert', 35), ('james', 25)]
#
# df=spark.createDataFrame(data=lst, schema=('Name string', 'Age Int'))
# df.printSchema()
# df.show()

# dict =[{"name": "robert", "age": 25}, {"name": "ankit", "age": 31}]
# df=spark.createDataFrame(data=dict)
# df.printSchema()
# df.show()

# rdd=sc.parallelize(lst)
# for i in rdd.collect():
#     print(i)
# df=spark.createDataFrame(data=rdd, schema=('Name String , Age Long'))
# df.printSchema()
# df.show()
# row=Row(name='ankit', Age=30)
# print(row.name)
# print(row.Age)
#
# rdd=spark.sparkContext.parallelize([row,Row(name='vinay', Age=27)])
# for i in rdd.collect():
#     print(i)
# df=spark.createDataFrame(data=rdd)
# df.printSchema()
# df.show()
# data = [['tom', 10], ['dick', 15], ['harry', 20]]
# df_pandas = pd.DataFrame(data, colums=['name', 'Age'])
# df = spark.createDataFrame(data=df_pandas)
# df.printSchema()
# df.show()
spark.stop()
