# IMPORTING
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

spark = SparkSession.builder.master('local').appName('SET EXAMPLE ').getOrCreate()

print(""" SPARK OBJECT IS CREATED """)
print(""" **************************** """)

data = [("James", "Sales", "NY", 9000, 34),
        ("Alica", "Sales", "NY", 8600, 56),
        ("Robert", "Sales", "CA", 8100, 30),
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

# AVERAGE SALARY
df.groupby(df.dept).avg("salary").alias('Average_salary').show()

# SUM OF SALARY
df.groupby(df.dept).sum("salary").alias('Sum_of_salary').show()

# MAX AND MIN OF SALARY
df.groupby(df.dept).max("salary").alias('max_of_salary').show()
df.groupby(df.dept).min("salary").alias('min_of_salary').show()

# GROUPING MULTIPLE COLUMNS
df.groupby(df.dept, df.state).min("salary", "age").show()

# USING AGG() FOR MANY AGGREGATE
df.groupBy(df.dept).agg(min('salary'), max('salary'), avg('salary')).show()

# USING AGG() FOR MANY AGGREGATE AND APPLYING FILTER
df.groupBy(df.dept).agg(min('salary'), max('salary'), avg('salary')).where(
    df.state == 'NY').show()  # this you cannot do

df.where(
    df.state == 'NY').groupBy(df.dept).agg(min('salary'), max('salary'), avg('salary')).show() # this you can do

# PIVOT
df.groupBy(df.dept).pivot("sate").sum("salary").show()

