import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def save_final_clean_data(dataframe,location,file_type):
    dataframe.write\
    .format(file_type)\
    .option("header", "true")\
    .mode("overwrite")\
    .save(location)

def save_final_clean_data_with_repart(dataframe,location,file_type):
    dataframe.repartition(1).write\
    .format(file_type)\
    .option("header", "true")\
    .mode("overwrite")\
    .save(location)

def save_bad_data(dataframe,location,file_type):
    dataframe.repartition(1).write\
    .format(file_type)\
    .option("header", "true")\
    .mode("overwrite")\
    .save(location)