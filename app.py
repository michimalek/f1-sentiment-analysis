from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from google.cloud import storage
import pandas as pd

def download_csv(csv_name):
    bucket_path = "de2022_f1"
    client = storage.Client()
    bucket = client.bucket(bucket_path)
    if isinstance(csv_name, list):
        for csv in csv_name:
            bucket.blob(csv).download_to_filename(f"data/{csv}")
    else:
        bucket.blob(csv_name).download_to_filename(f"data/{csv_name}")

download_csv(["races.csv","drivers.csv", "results.csv", "F1_tweets.csv"])

f1_tweet_data_path = "data/F1_tweets.csv"
f1_races_path = "data/races.csv"
f1_score_path = "data/results.csv"
f1_divers_path = "data/drivers.csv"

# Create Spark session
spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "500mb") \
    .appName("app") \
    .getOrCreate()

def get_data(file_name):
    return spark.read.csv(file_name, header=True);

tweets = get_data(f1_tweet_data_path);
races = get_data(f1_races_path);
scores = get_data(f1_score_path);
drivers = get_data(f1_divers_path);

df = scores.join(races.alias("races"), scores.raceId ==  races.raceId,"left") \
     .orderBy(col("races.raceId")) \
     .join(drivers, scores.driverId == drivers.driverId, "left") \
     .join(races.alias("prevRace"), col("prevRace.raceId") == scores.raceId - 1, "left") \
     .filter(col("races.year") == 2021) \
     .filter(scores.position == 1) \
     .select(scores.raceId, col("races.name"), drivers.surname, col("races.date").alias("to_date"), col("prevRace.date").alias("from_date")).show()


# tweets.filter(tweets.date >= df.collect()[0][4]) \
#     .filter(tweets.date <= df.collect()[0][3]).show()

# races.
# users_table.groupby(col("date")).agg(countDistinct(col("product_id")).alias("distinct_products_sold")).orderBy(
#     col("distinct_products_sold").desc()).show()