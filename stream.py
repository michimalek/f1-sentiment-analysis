from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructField, LongType, DoubleType, IntegerType
from pyspark.sql.functions import *
import time

dataSchema = StructType(
        [StructField("year", IntegerType(), True),
         StructField("url", LongType(), True),
         ])


# Read from a source 
spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "500mb") \
    .appName("stream") \
    .getOrCreate()

sdf = spark.readStream.schema(dataSchema).option("maxFilesPerTrigger", 1) \
        .csv("/Users/jonathan/Documents/JADS/year-1/data-engineering/DE-assignment-2/work_dir/data/stream")

activityCounts = sdf.groupBy("year").count()

activityQuery = activityCounts.writeStream.queryName("activity_counts") \
                    .format("memory").outputMode("complete") \
                    .start()

for x in range(40):
    spark.sql("SELECT * FROM activity_counts").show()
    time.sleep(10) # Sleep for 5 seconds      

try:
    activityQuery.awaitTermination()
except KeyboardInterrupt:
    activityQuery.stop()
    # Stop the spark context
    spark.stop()
    print("Stoped the streaming query and the spark context")              