from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time, threading
import logging

# Spark Session
spark = SparkSession.builder.appName("RedditStreamingApp").getOrCreate()

# Define schema
schema = StructType([
    StructField("text", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("created_utc", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])


# Read data from Kafka
reddit_df_1 = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "reddit_tweets_1").load()
reddit_df_1 = reddit_df_1.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

reddit_df_2 = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "reddit_tweets_2").load()
reddit_df_2 = reddit_df_2.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

reddit_df_3 = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "reddit_tweets_3").load()
reddit_df_3 = reddit_df_3.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

def write_to_mysql(df, epoch_id, table_name):
    df.write.format('jdbc').options(
          url='jdbc:mysql://localhost:3306/dbtproj',
          driver='com.mysql.jdbc.Driver',
          dbtable=table_name,
          user='root',
          password='varunbwaj').mode('append').save()

# Write data to MySQL
query1 = reddit_df_1.writeStream.outputMode("append").foreachBatch(lambda df, epoch_id: write_to_mysql(df, epoch_id, 'reddit_data_1')).start()
query2 = reddit_df_2.writeStream.outputMode("append").foreachBatch(lambda df, epoch_id: write_to_mysql(df, epoch_id, 'reddit_data_2')).start()
query3 = reddit_df_3.writeStream.outputMode("append").foreachBatch(lambda df, epoch_id: write_to_mysql(df, epoch_id, 'reddit_data_3')).start()

# Measure performance (execution time, memory usage, etc.)
start_time = time.time()


# Combine all dataframes
combined_df = reddit_df_1.union(reddit_df_2).union(reddit_df_3)

# Perform aggregation
aggregated_df = combined_df.groupBy("subreddit").agg(avg("score").alias("average_score"))

# Add a timestamp column
aggregated_df = aggregated_df.withColumn("timestamp", current_timestamp())

def write_to_mysql(df, epoch_id, table_name):
    df.write.format('jdbc').options(
          url='jdbc:mysql://localhost:3306/dbtproj',
          driver='com.mysql.jdbc.Driver',
          dbtable=table_name,
          user='root',
          password='varunbwaj').mode('append').save()

# Write aggregated data to MySQL
query4 = aggregated_df.writeStream.outputMode("complete").foreachBatch(lambda df, epoch_id: write_to_mysql(df, epoch_id, 'reddit_avg_score')).start()

end_time = time.time()
# end_memory = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss  # in KB

# Function to stop queries after  16 minutes
def stop_queries():
    time.sleep(60*16)
    query1.stop()
    query2.stop()
    query3.stop()
    query4.stop()

# Start a separate thread to stop the queries
threading.Thread(target=stop_queries).start()

query4.awaitTermination()
query1.awaitTermination()
query2.awaitTermination()
query3.awaitTermination()

# Measure performance (execution time, memory usage, etc.)

execution_time = end_time - start_time

from pyspark.sql.functions import desc

# Read data from MySQL in batch mode
streamed_df = spark.read.format('jdbc').options(
          url='jdbc:mysql://localhost:3306/dbtproj',
          driver='com.mysql.jdbc.Driver',
          dbtable='reddit_avg_score',
          user='root',
          password='varunbwaj').load()

# Get the latest 3 entries based on timestamp
latest_entries_df = streamed_df.orderBy(desc("timestamp")).limit(3)

# Open the output.txt file in append mode
with open("output.txt", "a") as f:
    # Print the results to both the console and the output.txt file
    print("=============================Streaming Mode Results:===============================", file=f)
    print(f"Execution time: {execution_time} seconds", file=f)
    print(latest_entries_df.show(), file=f)
    print("===================================================================================", file=f)

    # And so on for all other print statements...

print("=============================Stream Mode Results:===============================")
print(f"Execution time: {execution_time} seconds")
latest_entries_df.show()
print("===================================================================================")

# print(f"Memory usage: {memory_usage} KB")

# Batch Mode Execution
# Read last 10 rows from MySQL
query = "SELECT * FROM reddit_data_1 "
reddit_df_1 = spark.read.format('jdbc').options(
          url='jdbc:mysql://localhost:3306/dbtproj',
          driver='com.mysql.jdbc.Driver',
          dbtable="reddit_data_1 ",
          user='root',
          password='varunbwaj').load()

query = "SELECT * FROM reddit_data_2 "
reddit_df_2 = spark.read.format('jdbc').options(
          url='jdbc:mysql://localhost:3306/dbtproj',
          driver='com.mysql.jdbc.Driver',
          dbtable="reddit_data_2",
          user='root',
          password='varunbwaj').load()

query = "SELECT * FROM reddit_data_3"
reddit_df_3 = spark.read.format('jdbc').options(
          url='jdbc:mysql://localhost:3306/dbtproj',
          driver='com.mysql.jdbc.Driver',
          dbtable="reddit_data_3",
          user='root',
          password='varunbwaj').load()

start_time = time.time()

# Combine all DataFrames
combined_df = reddit_df_1.union(reddit_df_2).union(reddit_df_3)

# Perform aggregation
batch_aggregated_df = combined_df.groupBy("subreddit").agg(avg("score").alias("average_score"))

end_time = time.time()

# Open the output.txt file in append mode
with open("output.txt", "a") as f:
    # Print the results to both the console and the output.txt file
    print("=============================Batch Mode Results:===============================", file=f)
    print(f"Execution time: {execution_time} seconds", file=f)
    print(batch_aggregated_df.show(), file=f)
    print("===================================================================================", file=f)



print("=============================Batch Mode Results:===============================")
batch_aggregated_df.show()
execution_time = end_time - start_time
print(f"Execution time: {execution_time} seconds")
print("===================================================================================")