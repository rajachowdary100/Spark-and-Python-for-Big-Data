# importing required libraries
# importing required libraries
import numpy as np
import pandas as pd
import seaborn as sns
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('missing').getOrCreate()
df = spark.read.csv('datasets/ContainsNull.csv',inferSchema=True,header=True)
df.show()
df.printSchema()
# dropping any row containing null values
df.na.drop().show()
# specifying threshold--so that it will drop rows having null values less than this value
df.na.drop(thresh=2).show()

# filling the null values-this will fill all the null values in the dataframe
df.na.fill('fill value').show() # this will be inserted into string datatype column as it is a string
df.na.fill(0.0).show() # this will be inserted into double/number datatype column as it is a number

# imputing with mean
from pyspark.sql.functions import mean
mean_val = df.select(mean(df['Sales'])).collect()
mean_sales = mean_val[0][0]
print(mean_sales) # for grabbing only the value
df.na.fill(mean_sales,["Sales"]).show()
