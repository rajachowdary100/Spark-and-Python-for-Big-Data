# importing required libraries
import numpy as np
import pandas as pd
import seaborn as sns
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('regression').getOrCreate()
from pyspark.ml.regression import LinearRegression

# Loading the data

df_train = spark.read.format('libsvm').load('datasets/sample_linear_regression_data.txt') # this is how spark needs the
# data to run a ml model
df_train.show()








