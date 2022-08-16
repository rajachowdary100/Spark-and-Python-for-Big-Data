# importing required libraries
import numpy as np
import pandas as pd
import seaborn as sns
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('sparkDataframe').getOrCreate()
# reading the json file
df = spark.read.json("datasets/people.json")
print(df.show())
# checking the schema of the dataframe
print(df.printSchema())

# checking the columns
print(df.columns)
# checking the summary statistics of the dataframe
df.describe().show()

# Manual setting of the dataframe schema
# Defining/altering the schema-VERY IMPORTANT FOR LARGER DATASETS
from pyspark.sql.types import (StructField,StringType,
                               IntegerType,StructType)
# True indicates that it can be nullable
data_schema = [StructField('age',IntegerType(),True),
               StructField('name',StringType(),True)]
final_struc = StructType(fields=data_schema)
df = spark.read.json('datasets/people.json',schema=final_struc)
df.show()
df.printSchema()

# Grabbing data from the dataframe
# use select function to select the columns
df.select('age').show()
print(type(df.head(2)[0])) # head function indicates row in pyspark

# Adding a new column to the dataframe
df.withColumn('newage',df['age']).show() # this returns the dataframe with the copy of the existing column
# we can also perform certain operations over the existing column
df.withColumn('double_age',df['age']*2).show() # this is not an inplace operation(didn't modify the existing df)

# Renaming the column names--not an inplace operation
df.withColumnRenamed('age','my_new_age').show()

# few more examples of adding new column
df.withColumn('half_age',df['age']/2).show()
df.withColumn('add_age',df['age']+2).show()

# Using direct sql queries over a dataframe
# this will register the df as sql table/sql temporary view
df.createOrReplaceTempView('people')
results = spark.sql("select * from people")
results.show()
new_results = spark.sql('select * from people where age=30')
new_results.show()

