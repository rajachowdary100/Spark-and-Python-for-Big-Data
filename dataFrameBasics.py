# importing required libraries
import numpy as np
import pandas as pd
import seaborn as sns
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('sparkDataframe').getOrCreate()
# reading the json file
df = spark.read.json("people.json")
print(df.show())
# checking the schema of the dataframe
print(df.printSchema())

# checking the columns
print(df.columns)
# checking the summary statistics of the dataframe
print(df.describe().show())

# Manual setting of the dataframe schema
# Defining/altering the schema-VERY IMPORTANT FOR LARGER DATASETS
from pyspark.sql.types import (StructField,StringType,
                               IntegerType,StructType)
# True indicates that it can be nullable
data_schema = [StructField('age',IntegerType(),True),
               StructField('name',StringType(),True)]
final_struc = StructType(fields=data_schema)
df = spark.read.json('people.json',schema=final_struc)
print(df.show())
print(df.printSchema())

