import praw
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from kafka import KafkaProducer
import json
import time
import traceback

# Reddit API credentials
reddit = praw.Reddit(client_id='7rae84HhPjnd9P1382BH9A',
                     client_secret='bXdbWNh__ibW6CmVvsSgbnynUk_h-w',
                     user_agent='dbtAssignmentv1')

# Spark Session
spark = SparkSession.builder.appName("RedditStreamingApp").getOrCreate()

# Kafka Producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Streaming Function
def process_stream(batch_id, df):
    print("========= %s =========" % str(batch_id))
    try:
        # Get the single element in the RDD (since groupBy() is used)
        rdd_list = df.rdd.collect()
        
        # Filter out empty batches
        if len(rdd_list) > 0:
            df = spark.createDataFrame(rdd_list[0], schema=["text", "subreddit", "score", "num_comments"])
            
            # Example Operations
            
            # Count tweets within a specified window
            count_tweets = df.count()
            print(f"Total tweets in this window: {count_tweets}")
            
            # Group tweets by subreddit
            subreddit_counts = df.groupBy("subreddit").count().orderBy("count", ascending=False)
            print("Tweets grouped by subreddit:")
            subreddit_counts.show(truncate=False)
            
            # Apply aggregate function (e.g., max) on numerical data
            # max_score = df.agg({"score": "max"}).collect()[0][0]
            max_score = df.agg({"score": "max"}).head()[0]
            print(f"Maximum score in this window: {max_score}")
            
            # Store tweets into Kafka for further processing
            for row in df.collect():
                tweet_data = {"text": row["text"], "subreddit": row["subreddit"], "score": row["score"], "num_comments": row["num_comments"]}
                producer.send('reddit_tweets', value=tweet_data)
            
            # Store tweets into a MySQL database for batch processing
            df.write.format("jdbc").option("url", "jdbc:mysql://localhost:3306/dbtproj").option("dbtable", "reddit_tweets").option("user", "your_username").option("password", "your_password").mode("append").save()
    except Exception as e:
        print(e)
        

# Reddit Stream
# reddit_stream = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
reddit_stream = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
reddit_stream = reddit_stream.select(reddit_stream["value"].cast("string"))

# Define schema
schema = StructType([StructField("text", StringType(), True),
                     StructField("subreddit", StringType(), True),
                     StructField("score", IntegerType(), True),
                     StructField("num_comments", IntegerType(), True)])

# Apply schema and window operations
reddit_df = reddit_stream.select(from_json(col("value"), schema).alias("data")).select("data.*")
reddit_df = reddit_df.withColumn("timestamp", current_timestamp())
reddit_df = reddit_df.withWatermark("timestamp", "30 minutes")

window_size = "30 minutes"
windowed_df = reddit_df.groupBy(window(reddit_df.timestamp, window_size)).agg(collect_list("text").alias("text"),
                                                                               collect_list("subreddit").alias("subreddit"),
                                                                               collect_list("score").alias("score"),
                                                                               collect_list("num_comments").alias("num_comments"))

# Process stream
streaming_start_time = time.time()
# query = windowed_df.writeStream.foreachBatch(process_stream).start()
# query = windowed_df.writeStream.outputMode("appendAsUpdate").foreachBatch(process_stream).start()
query = windowed_df.writeStream.outputMode("update").foreachBatch(process_stream).start()
query.awaitTermination()
streaming_end_time = time.time()
streaming_execution_time = streaming_end_time - streaming_start_time

# Batch Mode Processing
def process_batch(spark, database_url, username, password):
    # Read data from the MySQL database
    df = spark.read.format("jdbc").option("url", database_url).option("user", username).option("password", password).load()

    print("reading done")

    batch_start_time = time.time()

    # Example Operations (same as streaming mode)
    count_tweets = df.count()
    print(f"Total tweets in batch mode: {count_tweets}")

    subreddit_counts = df.groupBy("subreddit").count().orderBy("count", ascending=False)
    print("Tweets grouped by subreddit (batch mode):")
    subreddit_counts.show(truncate=False)

    # max_score = df.agg({"score": "max"}).collect()[0][0]
    max_score = df.agg({"score": "max"}).head()[0]
    print(f"Maximum score in batch mode: {max_score}")

    batch_end_time = time.time()
    batch_execution_time = batch_end_time - batch_start_time
    print(f"Batch mode execution time: {batch_execution_time} seconds")

    return batch_execution_time

# Batch Mode Execution
batch_execution_time = process_batch(spark, "jdbc:mysql://localhost:3306/dbtproj", "root", "varunbwaj")

# Compare Results and Performance
print("\n===== Comparison =====")
print(f"Streaming mode execution time: {streaming_execution_time} seconds")
print(f"Batch mode execution time: {batch_execution_time} seconds")

# Additional comparisons or evaluations can be added here