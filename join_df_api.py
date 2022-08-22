# IMPORTING
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.master('local').appName('SET EXAMPLE ').getOrCreate()

print(""" SPARK OBJECT IS CREATED """)
print(""" **************************** """)

df1 = spark.createDataFrame(data=[(1, 'Robert', 2), (2, 'Ria', 3), (3, 'James', 5)], schema='empid int, empname '
                                                                                            'string, managerid int')
df2 = spark.createDataFrame(data=[(2, 102, 'USA'), (4, 104, 'INDIA')], schema='empid int, deptid int,  country string')
df3 = spark.createDataFrame(data=[(1, 101,  'Robert'), (2, 102, 'Ria'), (3, 103, 'James')], schema='empid int, deptid '
                                                                                                   'int, '
                                                                                                   'empname string')
df4 = spark.createDataFrame(data=[(1, '01-jan-2021'), (2, '01-feb-2021'), (3, '01-mar-2021') ], schema='empid int, '
                                                                                                       'joindate '
                                                                                                       'string')
df1.show()
df2.show()
df3.show()
df4.show()

# INNER JOIN
df1.join(df2, df1.empid == df2.empid).show()  # BY DEFAULT ITD INNER JOIN IF WE NOT MENTION ANY THING
df1.join(df2, df1.empid == df2.empid).select(df1.empid, df2.country).show()

# LEFT JOIN
df1.join(df2, df1.empid == df2.empid, 'left').show()
df1.join(df2, df1.empid == df2.empid, 'left').select(df1.empid, df2.country).show()

# LEFT_ANTI JOIN
df1.join(df2, df1.empid == df2.empid, 'leftanti').show()
df1.join(df2, df1.empid == df2.empid, 'leftanti').select(df1.empid, df2.country).show()

# LEFT_SEMI JOIN
df1.join(df2, df1.empid == df2.empid, 'leftsemi').show()

# RIGHT JOIN
df1.join(df2, df1.empid == df2.empid, 'right').show()
df1.join(df2, df1.empid == df2.empid, 'right').select(df1.empid, df2.country).show()

# FULL JOIN
df1.join(df2, df1.empid == df2.empid, 'full').show()
df1.join(df2, df1.empid == df2.empid, 'full').select(df1.empid, df2.country).show()

# CROSS JOIN
df1.join(df2, df1.empid == df1.empid, 'cross').show()
df1.crossJoin(df2).show()

# SELF JOIN

df1.alias("emp1").join(df1.alias("emp2"), col("emp1.managerid") == col("emp2.empid"), "inner").show()

df1.alias("emp1").join(df1.alias("emp2"), col("emp1.managerid") == col("emp2.empid"), "inner").\
    select(col("emp1.empid"), col("emp1.empname"),
           col("emp2.empid").alias("manager_id"), col("emp2.empname").alias("manager_name")).show()


# MULTI COLUMN JOIN
df3.join(df2, (df3.empid == df2.empid) & (df3.deptid == df2.deptid), 'inner').show()

# MULTI DATAFRAME JOIN
df1.join(df2, df1.empid == df2.empid, 'inner').join(df4, df1.empid == df4.empid, 'inner').show()
