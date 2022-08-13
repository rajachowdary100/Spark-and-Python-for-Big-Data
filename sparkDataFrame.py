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