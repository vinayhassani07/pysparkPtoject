from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pyspark

spark = SparkSession.builder.master('local').appName('Data Type Example').getOrCreate()
# spark.sql.legacy.createHiveTableByDefault = False
print(""" Spark Object is Created """)

# tbl=spark.sql(""" Create Table test(c1 string , c2 varchar(10) , c3 char(10) )""")
#
# tbl.show()
#
# insert= spark.sql(""" insert into test values ("robert","robert","robert") """)
#
# tbl.show()
#
# spark.sql(""" select * from test""").show()
#
# spark.sql(""" select length(c1) string_len , length(c2) varchar_len ,length(31) char_len from test """).show()

lst1 = [('robert', 35), ('james', 25)]

lst2 = [('robert', 101), ('james', 102)]

df_emp = spark.createDataFrame(data=lst1, schema=['empName', 'Age'])

df_emp.printSchema()

df_emp.show()

df_dept = spark.createDataFrame(data=lst2, schema=['empName', 'DeptNo'])

df_emp.createOrReplaceTempView("emp")  # active for one session

# Now We will do this with struct type


schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField("name", pyspark.sql.types.StringType(), True),
    pyspark.sql.types.StructField("id", pyspark.sql.types.IntegerType(), True),
])

data = [
    ("james", 1),
    ("robert", 2),
    ("maria", 3)
]

df = spark.createDataFrame(data=data, schema=schema)

df.printSchema()

df.show(truncate=False)

# MAP TYPE

schema_map = StructType([
    StructField('name', StringType(), True),
    StructField('Properties', MapType(StringType(), StringType(), True))
])

d = [
    ("james", {'hair': 'black', 'eye': 'brown'}),
    ("robert", {'hair': 'brown', 'eye': 'none'}),
    ("maria", {'hair': 'red', 'eye': 'black'})
]

df_map = spark.createDataFrame(data=d, schema=schema_map)

df_map.printSchema()

df_map.show(truncate=False)

# ARRAY TYPE

schema_arry = StructType([
    StructField('name', StringType(), True),
    StructField('MobileNumber', ArrayType(IntegerType()), True)
])

d1 = [
    ("james", [123, 456, 789]),
    ("robert", [234, 456, 678]),
    ("maria", [168, 89, 190])
]




df_arr = spark.createDataFrame(data=d1, schema=schema_arry)

df_arr.printSchema()

df_arr.show(truncate=False)
