# importing required libraries
import numpy as np
import pandas as pd
import seaborn as sns
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('aggregate').getOrCreate()
df = spark.read.csv('datasets/sales_info.csv',inferSchema=True,header=True)
df.show()
df.printSchema()

# grouping by company
df.groupBy('Company').mean().show()
# sum
df.groupBy('Company').sum().show()
# max
df.groupBy('Company').max().show()
# min
df.groupBy('Company').min().show()
# count
df.groupBy('Company').count().show()

# Generalized aggregate function to apply an aggregate function instead of using groupBy
# calculating sum of all the sales
# will operate over a numerical value
# takes in dictionary with key being the column to be aggregated
df.agg({'Sales':'sum'}).show()
# max
df.agg({'Sales':'max'}).show()
# min
df.agg({'Sales':'min'}).show()
# same can be done using groupby object
df_grouped = df.groupBy('Company')
df_grouped.agg({'Sales':'mean'}).show()
df_grouped.agg({'Sales':'max'}).show()
df_grouped.agg({'Sales':'min'}).show()
df_grouped.agg({'Sales':'sum'}).show()

# to use several functions,we have to import them from pyspark.sql.functions module
from pyspark.sql.functions import countDistinct,avg,stddev
# count distinct values in a column of a dataframe
df.select(countDistinct('Sales')).show()
df.select(countDistinct('Company')).show()
# calculating average using in-built function
df.select(avg('Sales')).show()
# renaming the column by using alias() just as in sqlalchemy
df.select(avg('Sales').alias('Average Sales')).show()
# calculating the standard deviation of sales column
df.select(stddev('Sales')).show()
# renaming using alias()
df.select(stddev('Sales').alias('Standard Deviation')).show()
# formatting the decimal numbers in the result set
from pyspark.sql.functions import format_number
sales_std = df.select(stddev('Sales').alias('std'))
sales_std.select(format_number('std',2).alias('Standard Deviation')).show()
# using orderby-by default ascending
df.orderBy("Sales").show()
# for descending we have to pass the column itself but not the column name
df.orderBy(df['Sales'].desc()).show()
