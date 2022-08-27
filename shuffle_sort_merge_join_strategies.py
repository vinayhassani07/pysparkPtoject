# PREPARING THE DATA
import pyspark.context
from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf().setMaster("local").setAppName("My App")
sc = pyspark.context.SparkContext(conf=conf)

spark = SparkSession.builder.master('local').appName('Column Example').getOrCreate()

print(""" SPARK OBJECT IS CREATED """)
print(""" **************************** """)


print(" **** PROPERTIES *** " + str(spark.conf.set("spark.sql.join.preferSortMergeJoin", )))

df1= spark.range(1, 10000000000)
df2= spark.range(1, 10000000)

joined= df1.join(df2, "id")
joined.explain()
