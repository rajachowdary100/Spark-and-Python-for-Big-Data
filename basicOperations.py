# importing required libraries
import numpy as np
import pandas as pd
import seaborn as sns
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('basicOps').getOrCreate()

df = spark.read.csv('datasets/appl_stock.csv',inferSchema=True,header=True)
df.show()
df.printSchema()
print(df.head(3)[0])

# Filtering the dataframe based on certain logical conditions
# scanario-grab al the data that has the closing price less than 500
# using direct sql syntax
df.filter('Close < 500').show()
# selecting some column after the above filter
df.filter('Close < 500').select('Open').show()
# for selecting multiple columns, pass a list
df.filter('Close < 500').select(['Open','Close']).show()

# using dataframe/python syntax
df.filter(df['Close'] < 500).show()
# selecting columns after this
df.filter(df['Close'] < 500).select('Volume').show()

# filtering based on multiple conditions
# scenario-grab all the rows having closing price less than 200 and open price greater than 200
# Caution:both 'and' and '&'(without parenthesis) gives error
df.filter((df['Close'] < 200) & (df['Open'] > 200)).select(['Open','Close','Volume']).show()
# adding multiple filter functions(same as sqlalchemy orm syntax)
df.filter((df['Close'] < 200) | (df['Open'] > 200)).\
    filter(df['Volume']==220441900).\
    select(['Open','Close','Volume']).show()
# for not condition we have to use "~"(with parenthesis) same as in pandas
df.filter((df['Close'] < 200) | (df['Open'] > 200)).\
    filter(~(df['Volume']==220441900)).\
    select(['Open','Close','Volume']).show()
# another example
# scenario-what date was low price as $197.16
df.filter(df['Low']==197.16).\
    select('Date').show()
# using not
df.filter(~(df['Low']==197.16)).\
    select('Date').show()
# to get the result as a row format using list use collect() instead of show()
# use print() for list object
print(df.filter(df['Low']==197.16).collect())
rows = df.filter(df['Low']==197.16).collect()
print(rows[0])
# there are also few methods for the rows
# convert into a dictionary
row = rows[0]
row_dict = row.asDict() # used in most cases to fetch the values
print(row_dict['Volume'])
