# IMPORTING
from pyspark.sql import SparkSession
import pyspark.sql.functions
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
df.select(df.dept, df.salary). \
    withColumn("row_number_rank", pyspark.sql.functions.row_number().over(spec)).show()

# RANK
df.select(df.dept, df.salary). \
    withColumn("row_number_rank", pyspark.sql.functions.row_number().over(spec)). \
    withColumn("rank", pyspark.sql.functions.rank().over(spec)).show()

# DENSE RANK
df.select(df.dept, df.salary). \
    withColumn("row_number_rank", pyspark.sql.functions.row_number().over(spec)). \
    withColumn("rank", pyspark.sql.functions.rank().over(spec)). \
    withColumn("dense_rank", pyspark.sql.functions.dense_rank().over(spec)).show()

# PERFECT RANK
df.select(df.dept, df.salary). \
    withColumn("row_number_rank", pyspark.sql.functions.row_number().over(spec)). \
    withColumn("rank", pyspark.sql.functions.rank().over(spec)). \
    withColumn("dense_rank", pyspark.sql.functions.dense_rank().over(spec)). \
    withColumn("percent_rank", pyspark.sql.functions.percent_rank().over(spec)). \
    show()

# NTILE RANK
df.select(df.dept, df.salary). \
    withColumn("row_number_rank", pyspark.sql.functions.row_number().over(spec)). \
    withColumn("rank", pyspark.sql.functions.rank().over(spec)). \
    withColumn("dense_rank", pyspark.sql.functions.dense_rank().over(spec)). \
    withColumn("percent_rank", pyspark.sql.functions.percent_rank().over(spec)). \
    withColumn("ntile_rank", pyspark.sql.functions.ntile(3).over(spec)). \
    show()

# CUME_DIST
df.select(df.dept, df.salary). \
    withColumn("row_number_rank", pyspark.sql.functions.row_number().over(spec)). \
    withColumn("rank", pyspark.sql.functions.rank().over(spec)). \
    withColumn("dense_rank", pyspark.sql.functions.dense_rank().over(spec)). \
    withColumn("percent_rank", pyspark.sql.functions.percent_rank().over(spec)). \
    withColumn("ntile_rank", pyspark.sql.functions.ntile(3).over(spec)). \
    withColumn("cum_dist", pyspark.sql.functions.cume_dist().over(spec)). \
    show()

# LAG FUNCTION
df.select(df.dept, df.salary). \
    withColumn("row_number_rank", pyspark.sql.functions.row_number().over(spec)). \
    withColumn("rank", pyspark.sql.functions.rank().over(spec)). \
    withColumn("dense_rank", pyspark.sql.functions.dense_rank().over(spec)). \
    withColumn("percent_rank", pyspark.sql.functions.percent_rank().over(spec)). \
    withColumn("ntile_rank", pyspark.sql.functions.ntile(3).over(spec)). \
    withColumn("cum_dist", pyspark.sql.functions.cume_dist().over(spec)). \
    withColumn("lag_prev_sal", pyspark.sql.functions.lag(df.salary, 1, 0).over(spec)). \
    show()

# LEAR FUNCTION
df.select(df.dept, df.salary). \
    withColumn("row_number_rank", pyspark.sql.functions.row_number().over(spec)). \
    withColumn("rank", pyspark.sql.functions.rank().over(spec)). \
    withColumn("dense_rank", pyspark.sql.functions.dense_rank().over(spec)). \
    withColumn("percent_rank", pyspark.sql.functions.percent_rank().over(spec)). \
    withColumn("ntile_rank", pyspark.sql.functions.ntile(3).over(spec)). \
    withColumn("cum_dist", pyspark.sql.functions.cume_dist().over(spec)). \
    withColumn("lag_prev_sal", pyspark.sql.functions.lag(df.salary, 1, 0).over(spec)). \
    withColumn("lead_prev_sal", pyspark.sql.functions.lead(df.salary, 2, 9999).over(spec)). \
    show()

# AGGREGATE FUNCTION
df.select(df.dept, df.salary). \
    withColumn("row_number_rank", pyspark.sql.functions.row_number().over(spec)). \
    withColumn("rank", pyspark.sql.functions.rank().over(spec)). \
    withColumn("dense_rank", pyspark.sql.functions.dense_rank().over(spec)). \
    withColumn("percent_rank", pyspark.sql.functions.percent_rank().over(spec)). \
    withColumn("ntile_rank", pyspark.sql.functions.ntile(3).over(spec)). \
    withColumn("cum_dist", pyspark.sql.functions.cume_dist().over(spec)). \
    withColumn("lag_prev_sal", pyspark.sql.functions.lag(df.salary, 1, 0).over(spec)). \
    withColumn("lead_prev_sal", pyspark.sql.functions.lead(df.salary, 2, 9999).over(spec)).show()
# withColumn("sum_sal", sum(df.salary).over(spec)). \
# withColumn("max_sal", max(df.salary).over(spec)). \
# withColumn("min_sal", min(df.salary).over(spec)). \
# withColumn("avg_sal", avg(df.salary).over(spec)). \
# withColumn("count_sal", count(df.salary).over(spec)). \


# RANGE BETWEEN FUNCTION
spec1 = Window.partitionBy(df.dept).orderBy(df.salary).rangeBetween(Window.unboundedPreceding,
                                                                    Window.unboundedFollowing)
spec11 = Window.partitionBy(df.dept).orderBy(df.salary).rangeBetween(Window.currentRow,
                                                                     Window.unboundedFollowing)
spec111 = Window.partitionBy(df.dept).orderBy(df.salary).rangeBetween(Window.currentRow,500)

df.select(df.dept, df.salary).withColumn("sum_sal", pyspark.sql.functions.sum(df.salary).over(spec1)).show()
df.select(df.dept, df.salary).withColumn("sum_sal", pyspark.sql.functions.sum(df.salary).over(spec11)).show()
df.select(df.dept, df.salary).withColumn("sum_sal", pyspark.sql.functions.sum(df.salary).over(spec111)).show()

# ROW BETWEEN FUNCTION
spec2 = Window.partitionBy(df.dept).orderBy(df.salary).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
spec22 = Window.partitionBy(df.dept).orderBy(df.salary).rowsBetween(Window.currentRow, Window.unboundedFollowing)
spec222 = Window.partitionBy(df.dept).orderBy(df.salary).rowsBetween(Window.currentRow, 2)

df.select(df.dept, df.salary).withColumn("sum_sal", pyspark.sql.functions.sum(df.salary).over(spec2)).show()
df.select(df.dept, df.salary).withColumn("sum_sal", pyspark.sql.functions.sum(df.salary).over(spec22)).show()
df.select(df.dept, df.salary).withColumn("sum_sal", pyspark.sql.functions.sum(df.salary).over(spec222)).show()
