from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import pandas as pd
import csv

spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "500mb") \
    .appName("app") \
    .getOrCreate()

def get_data(file_name):
    return spark.read.csv(file_name, header='true');


with open('/Users/jonathan/Documents/JADS/year-1/data-engineering/DE-assignment-2/work_dir/data/F1_tweets.csv') as file:
    reader = csv.DictReader(file, delimiter=",")
    lines = [row for row in reader]
    df = pd.DataFrame(lines)
    df.sample(10000).to_csv('data/stream/random.csv', mode = 'w', index=False)
