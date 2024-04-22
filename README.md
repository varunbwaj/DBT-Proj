### File 1: Reddit Data Streaming

#### 1. Reddit Data Fetching and Sending to Kafka
```python
# Fetch data from Reddit and send it to Kafka
while time.time() < end_time:
    for subreddit, topic in [('announcements', 'reddit_tweets_1'), ('funny', 'reddit_tweets_2'), ('worldnews', 'reddit_tweets_3')]:
        # Get the subreddit
        subreddit = reddit.subreddit(subreddit)

        # Get new submissions
        new_submissions = list(subreddit.new(limit=10 * iteration))

        # Filter out the posts we've already seen
        new_submissions = [post for post in new_submissions if post.id not in seen_posts]

        # Update the set of seen posts
        seen_posts.update(post.id for post in new_submissions)

        # Send the new posts to the producer
        for submission in new_submissions:
            tweet_data = {
                "text": submission.title,
                "subreddit": str(submission.subreddit),
                "score": submission.score,
                "num_comments": submission.num_comments,
                "created_utc": submission.created_utc,
                "timestamp": datetime.datetime.now().isoformat() 
            }
            producer.send(topic, value=tweet_data)
        print("data sent to kafka ",counter)
        counter+=1

    time.sleep(15)  # API rate limitations
    iteration += 1  # Increment the iteration count
```

#### 2. Spark Streaming Setup and Data Processing
```python
# Spark Session
spark = SparkSession.builder.appName("RedditStreamingApp").getOrCreate()

# Read data from Kafka
reddit_df_1 = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "reddit_tweets_1").load()

# Define schema
schema = StructType([
    StructField("text", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("created_utc", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Write data to MySQL
query1 = reddit_df_1.writeStream.outputMode("append").foreachBatch(lambda df, epoch_id: write_to_mysql(df, epoch_id, 'reddit_data_1')).start()

# Measure performance (execution time, memory usage, etc.)
```

#### 3. Aggregation and Writing Aggregated Data to MySQL
```python
# Combine all dataframes
combined_df = reddit_df_1.union(reddit_df_2).union(reddit_df_3)

# Perform aggregation
aggregated_df = combined_df.groupBy("subreddit").agg(avg("score").alias("average_score"))

# Write aggregated data to MySQL
query4 = aggregated_df.writeStream.outputMode("complete").foreachBatch(lambda df, epoch_id: write_to_mysql(df, epoch_id, 'reddit_avg_score')).start()
```

#### 4. Performance Measurement and Termination
```python
# Measure performance (execution time, memory usage, etc.)

# Function to stop queries after 16 minutes
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
```

### File 2: Batch Processing

#### 1. Reading Data from MySQL
```python
# Batch Mode Execution
# Read last 10 rows from MySQL
query = "SELECT * FROM reddit_data_1 "
reddit_df_1 = spark.read.format('jdbc').options(
          url='jdbc:mysql://localhost:3306/dbtproj',
          driver='com.mysql.jdbc.Driver',
          dbtable="reddit_data_1 ",
          user='root',
          password='varunbwaj').load()
```

#### 2. Aggregation
```python
# Combine all DataFrames
combined_df = reddit_df_1.union(reddit_df_2).union(reddit_df_3)

# Perform aggregation
batch_aggregated_df = combined_df.groupBy("subreddit").agg(avg("score").alias("average_score"))
```

#### 3. Output
```python
# Output
print("=============================Batch Mode Results:===============================")
batch_aggregated_df.show()
execution_time = end_time - start_time
print(f"Execution time: {execution_time} seconds")
print("===================================================================================")
```

#### 4. Performance Measurement
```python
# Measure performance
execution_time = end_time - start_time
``` 

In summary, File 1 focuses on real-time data streaming from Reddit to Kafka, Spark Streaming for processing, and MySQL for storage. File 2, on the other hand, deals with batch processing of Reddit data fetched from MySQL, aggregation, and output. Both files include performance measurement and termination logic.