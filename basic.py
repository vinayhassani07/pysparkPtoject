#  Create a spark Object
from pyspark.sql import SparkSession

spark= SparkSession \
    .builder \
    .master('local') \
    .appName("Python spark SQL Basic Example .") \
    .getOrCreate()

print("Spark Object is Created .....")
print("Number of partitions for shuffle changed to : " + str(spark.conf.get('spark.sql.shuffle.partitions')))
#print("The HDFS Path is " + spark.conf.get("spark.yarn.appMasterEnv.HDFS_PATH"))
spark.stop()
