# IMPORTING
from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, rank, dense_rank, percent_rank, ntile, cume_dist
from pyspark.sql.window import WindowSpec, Window

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

spec = Window.partitionBy(df.dept).orderBy(df.salary.desc())

df.select(df.dept, df.salary).show()

# ROW NUMBER
df.select(df.dept, df.salary).\
    withColumn("row_number_rank", row_number().over(spec)).show()

# RANK
df.select(df.dept, df.salary).\
    withColumn("row_number_rank", row_number().over(spec)).\
    withColumn("rank", rank().over(spec)).show()

# DENSE RANK
df.select(df.dept, df.salary).\
    withColumn("row_number_rank", row_number().over(spec)).\
    withColumn("rank", rank().over(spec)).\
    withColumn("dense_rank", dense_rank().over(spec)).show()

# PERFECT RANK
df.select(df.dept, df.salary).\
    withColumn("row_number_rank", row_number().over(spec)).\
    withColumn("rank", rank().over(spec)).\
    withColumn("dense_rank", dense_rank().over(spec)).\
    withColumn("percent_rank", percent_rank().over(spec)).\
    show()

# NTILE RANK
df.select(df.dept, df.salary).\
    withColumn("row_number_rank", row_number().over(spec)).\
    withColumn("rank", rank().over(spec)).\
    withColumn("dense_rank", dense_rank().over(spec)).\
    withColumn("percent_rank", percent_rank().over(spec)).\
    withColumn("ntile_rank", ntile(3).over(spec)).\
    show()

# CUME_DIST
df.select(df.dept, df.salary).\
    withColumn("row_number_rank", row_number().over(spec)).\
    withColumn("rank", rank().over(spec)).\
    withColumn("dense_rank", dense_rank().over(spec)).\
    withColumn("percent_rank", percent_rank().over(spec)).\
    withColumn("ntile_rank", ntile(3).over(spec)).\
    withColumn("cum_dist", cume_dist().over(spec)).\
    show()
