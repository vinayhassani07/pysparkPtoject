#  Create a spark Object
import pyspark
from pyspark.sql import SparkSession

conf = pyspark.SparkConf().setMaster("local").setAppName("My App")
sc = pyspark.SparkContext(conf=conf)

spark = SparkSession \
    .builder \
    .master('local') \
    .appName("Python SQL Basic Example .") \
    .getOrCreate()

print("Spark Object is Created .....")
lst1 = [('robert', 35), ('james', 25)]

lst2 = [('robert', 101), ('james', 102)]

df_emp = spark.createDataFrame(data=lst1, schema=['empName', 'Age'])

df_emp.printSchema()

df_emp.show()

df_dept = spark.createDataFrame(data=lst2, schema=['empName', 'DeptNo'])

df_emp.createOrReplaceTempView("emp")  # active for one session

spark.sql(""" Select * from emp """).show()

df_dept.createGlobalTempView("dept")  # active for all session

spark.sql(""" Select * from global_temp.dept where DeptNo =102 """).show()

df=spark.table("emp")

df.show()

col = sorted(df_emp.collect()) == sorted(df.collect())
print(" ***********")
print(col)




