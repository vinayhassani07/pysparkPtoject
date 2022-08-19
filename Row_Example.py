from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.master('local').appName('Row Example').getOrCreate()

print(""" Spark Object Is Created """)

print(""" *********************************************** """)

# 1. USING ROW OBJECT

row = Row(name='Robert', Age=35)

print(row.name)

print(row.Age)

schema = ['Name', 'Age']

data1 = [Row(name='Alice', Age=11), Row(name='Robert', Age=35)]

rdd1 = sc.parallelize(data1)

for i in rdd1.collect():
    print(i.Name + '' + str(i.Age))

df = spark.createDataFrame(data=data1)

df.printSchema()

# 2. USING CUSTOM CLASS FOR ROW

Person=Row("name", "age")

p1=Person("james", 35)
p2=Person("alice", 11)

data2= [Person("james", 35), Person("alice", 11)]

rdd2= sc.parallelize(data2)

for i in rdd2.collect():
    print(i.Name + '' + str(i.Age))

df1= spark.createDataFrame(data=data2)

df1.printSchema()

df1.show()



