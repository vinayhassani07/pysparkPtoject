# IMPORTING
import pyspark.context
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, length

conf = SparkConf().setMaster("local").setAppName("My App")
sc = pyspark.context.SparkContext(conf=conf)

spark = SparkSession.builder.master('local').appName('Column Example').getOrCreate()

print(""" SPARK OBJECT IS CREATED """)
print(""" **************************** """)

# CREATING A DUMMY DATA
df=spark.range(1000000)
df=df.select(df.id, df.id*2, df.id*3)
df=df.union(df)
df=df.union(df)
df=df.union(df)

df.show(truncate=False)
