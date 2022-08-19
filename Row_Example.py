from pyspark.sql import  SparkSession
from pyspark.sql import  Row

spark= SparkSession.builder.master('local').appName('Row Example').getOrCreate()

print(""" Spark Object Is Created """)

print(""" *********************************************** """)

row=Row(name='Robert', Age=35)

print(row.name)

print(row.Age)

