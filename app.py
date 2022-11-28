from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *

f1_tweet_data_path = './data/F1_tweets.csv'
f1_score_path = './data/fr_stats/results.csv'

# Create Spark session
spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "500mb") \
    .appName("app") \
    .getOrCreate()

def get_data(file_name):
    return spark.read.csv(file_name, header='true');

tweets = get_data(f1_tweet_data_path);
scores = get_data(f1_score_path);

tweets.count();
# users_table.show();
# users_table.groupby(col('birthdate')).agg(avg('salary')).show();
# users_table.groupby(col("date")).agg(countDistinct(col("product_id")).alias("distinct_products_sold")).orderBy(
#     col("distinct_products_sold").desc()).show()