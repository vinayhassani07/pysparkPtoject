# IMPORTING
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, lit, concat, expr, spark_partition_id

spark = SparkSession.builder.master('local').appName('SET EXAMPLE ').getOrCreate()

print(""" SPARK OBJECT IS CREATED """)
print(""" **************************** """)

data = [("James", "Sales", "NY", 9000, 34),
        ("Alica", "Sales", "NY", 8600, 56),
        ("Robert", "Sales", "CA", 8100, 30),
        ("Jhon", "Sales", "AZ", 8600, 31),
        ("Ross", "Sales", "AZ", 8100, 33),
        ("Lisa", "Finance", "CA", 9000, 24),
        ("Deja", "Finance", "CA", 9900, 42),
        ("Susie", "Finance", "NY", 8300, 36),
        ("Ram", "Finance", "NY", 7900, 53),
        ("Kyle", "Marketing", "CA", 8000, 25),
        ("Riled", "Marketing", "NY", 9100, 50)
        ]

schema = ["empname", "dept", "state", "salary", "age"]

df = spark.createDataFrame(data=data, schema=schema)
df.show()

# monotonically_increasing_id
df.withColumn("id", monotonically_increasing_id()).show()

# lit
df.withColumn("country", lit("USA")).show()
df.withColumn("col1", concat(df.salary, lit('|'), df.age)).show()

# expr
df.withColumn("empname_len", expr("length(empname)")).show()
df.withColumn("age_desc", expr(" CASE when age > 50  then 'senior' else 'adult' end ")).show()

# spark_partition_id
df1=spark.range(10)
df1.rdd.getNumPartitions()
df1=df1.repartition(5)
df1.rdd.getNumPartitions()
df1.select("id", spark_partition_id()).show()