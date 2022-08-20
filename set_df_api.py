# IMPORTING
import pyspark.context
from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf().setMaster("local").setAppName("My App")
sc = pyspark.context.SparkContext(conf=conf)

spark = SparkSession.builder.master('local').appName('Column Example').getOrCreate()

print(""" SPARK OBJECT IS CREATED """)
print(""" **************************** """)


df1=spark.range(10)
df2=spark.range(5, 10)
df1.show()
df2.show()
# UNION
df1.union(df2).show()
# UNION ALL
df1.unionAll(df2).show()

# UNION BY NAME
df3=spark.createDataFrame(data=[('a', 1), ('b', 2)], schema='col1 string, col2 int')
df4=spark.createDataFrame(data=[(2, 'b'), (3, 'c')], schema='col2 int, col1 string')
df3.show()
df4.show()
df3.union(df4).show()
df3.unionByName(df4).show()
df3.unionByName(df4).distinct().show()

# INTERSECT & INTERSECT ALL
df5=spark.createDataFrame(data=[('a', 1), ('a', 1), ('b', 2)], schema='col1 string, col2 int')
df6=spark.createDataFrame(data=[('a', 1), ('a', 1), ('c', 2)], schema='col1 string, col2 int')
df5.show()
df6.show()
df5.intersect(df6).show()
df5.intersectAll(df6).show()

# EXPECT ALL
df1.exceptAll(df2).show()
