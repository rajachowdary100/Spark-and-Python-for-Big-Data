# importing required libraries
import numpy as np
import pandas as pd
import seaborn as sns
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('exercise').getOrCreate()
df = spark.read.csv('datasets/walmart_stock.csv',inferSchema=True,header=True)
df.show()
df.printSchema()



