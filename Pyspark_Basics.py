# importing required libraries
import numpy as np
import pandas as pd
import seaborn as sns
#import pyspark
#from pyspark.sql import SparkSession
#spark = SparkSession.builder.appName('sparkDataframe').getOrCreate()

#Creating a pandas dataframe
df = sns.load_dataset('tips')
print(df)

#Creating a spark dataframe
#sdf = spark.createDataFrame(df)
#print(sdf.show())
