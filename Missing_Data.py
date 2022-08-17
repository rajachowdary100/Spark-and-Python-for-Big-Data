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
