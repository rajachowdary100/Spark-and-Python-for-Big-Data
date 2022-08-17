# importing required libraries
import numpy as np
import pandas as pd
import seaborn as sns
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('exercise').getOrCreate()
df = spark.read.csv('datasets/walmart_stock.csv', inferSchema=True, header=True)

# what are the column names?
print(df.columns)

# What does the Schema look like?
df.printSchema()

# print out the first 5 columns
for x in df.head(5):
    print(x)

# Use describe() function
df.describe().show()

from pyspark.sql.types import (StructField, StringType,
                               IntegerType, StructType,
                               DecimalType, DoubleType)
from pyspark.sql.functions import format_number

# format the numbers
df_desc = df.describe()
# checking the schema
df_desc.printSchema()
df_desc.select([df_desc['Summary'],
               format_number(df_desc['Open'].cast("float"), 2).alias('Open'),
               format_number(df_desc['High'].cast("float"), 2).alias('High'),
               format_number(df_desc['Low'].cast("float"), 2).alias('Low'),
               format_number(df_desc['Close'].cast("float"), 2).alias('Close'),
               format_number(df_desc['Volume'].cast("float"), 2).alias('Volume'),
               format_number(df_desc['Adj Close'].cast("float"), 2).alias('Adj Close')]).show()

# Create a new column HV Ratio
df_1 = df.withColumn("HV Ratio",df['High']/df['Volume'])
df_1.select('HV Ratio').show()

# find the date having high price?
from pyspark.sql.functions import max
print(df.orderBy(df['High'].desc()).head(1)[0][0])

# to be continued