# importing required libraries
import numpy as np
import pandas as pd
import seaborn as sns
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Dates').getOrCreate()
df = spark.read.csv('datasets/appl_stock.csv',inferSchema=True,header=True)
df.show()
df.printSchema()
print(df.head(1))
df.select(["Date","Open"]).show()

# Extracting information from the date columns
# importing relevant functions to achieve this
from pyspark.sql.functions import (
                                    dayofmonth,hour,
                                    dayofyear,month,
                                    year,weekofyear,
                                    format_number,date_format
                                    )
# grabbing the day of the month
df.select(dayofmonth("Date")).show()
# grabbing the month
df.select(month("Date")).show()
# grabbing the year
df.select(year("Date")).show()
# grabbing the hour
df.select(hour("Date")).show()
# grabbing the week of the year
df.select(weekofyear("Date")).show()

# Adding new column
# saving a copy to another variable
new_df = df.withColumn("Year",year(df["Date"]))
# finding the mean per year using groupby()
new_df.groupBy("Year").mean().show() # by default this will apply to all the columns
# selecting only specific columns
new_df.groupBy("Year").mean().select(["Year","avg(Close)"]).show()

# formatting by saving to another variable
result = new_df.groupBy("Year").mean().select(["Year","avg(Close)"])
# renaming the column
new_result = result.withColumnRenamed("avg(Close)","Average Closing Price")
new_result.select(["Year",format_number("Average Closing Price",2).alias("Avg Close")]).show()