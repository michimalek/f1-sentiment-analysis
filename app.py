from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import pandas as pd

spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "500mb") \
    .appName("app") \
    .getOrCreate()

def get_data(file_name):
    return spark.read.csv(file_name, header='true');

data_path = 'data/F1_tweets.csv'
tweets = get_data(data_path);
tweets.printSchema()
