# IMPORTING
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.master('local').appName('SET EXAMPLE ').getOrCreate()

print(""" SPARK OBJECT IS CREATED """)
print(""" **************************** """)

df= spark.range(100)
df.show()

df.sample(fraction=0.3).show()

df.sample(fraction=0.3, withReplacement=True).show()

df.sample(fraction=0.3, withReplacement=True, seed=10).show()

df1=spark.range(100).select((col("id") % 3).alias("key"))

df1.show()

sampled= df1.sampleBy("key", fractions={0: 0.1, 1: 0.2}, seed=20)
sampled.show()

