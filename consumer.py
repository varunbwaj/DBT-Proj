# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# from pyspark.sql.types import *

# # Spark Session
# spark = SparkSession.builder.appName("RedditStreamingApp").getOrCreate()

# # Define schema
# schema = StructType([StructField("text", StringType(), True),
#                      StructField("subreddit", StringType(), True),
#                      StructField("score", IntegerType(), True),
#                      StructField("num_comments", IntegerType(), True)])

# # Read data from Kafka
# reddit_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "reddit_tweets").load()
# reddit_df = reddit_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# # Write data to console
# query = reddit_df.writeStream.outputMode("update").format("console").start()
# query.awaitTermination()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Spark Session
spark = SparkSession.builder.appName("RedditStreamingApp").getOrCreate()

# Define schema
schema = StructType([StructField("text", StringType(), True),
                     StructField("subreddit", StringType(), True),
                     StructField("score", IntegerType(), True),
                     StructField("num_comments", IntegerType(), True)])

# Read data from Kafka
reddit_df_1 = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "reddit_tweets_1").load()
reddit_df_1 = reddit_df_1.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

reddit_df_2 = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "reddit_tweets_2").load()
reddit_df_2 = reddit_df_2.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Write data to console
query1 = reddit_df_1.writeStream.outputMode("update").format("console").start()
query2 = reddit_df_2.writeStream.outputMode("update").format("console").start()

query1.awaitTermination()
query2.awaitTermination()