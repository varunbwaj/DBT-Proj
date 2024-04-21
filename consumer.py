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

reddit_df_3 = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "reddit_tweets_3").load()
reddit_df_3 = reddit_df_3.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Combine all dataframes
combined_df = reddit_df_1.union(reddit_df_2).union(reddit_df_3)

# Perform aggregation
aggregated_df = combined_df.groupBy("subreddit").agg(avg("score").alias("average_score"))

def write_to_mysql(df, epoch_id, table_name):
    df.write.format('jdbc').options(
          url='jdbc:mysql://localhost:3306/dbtproj',
          driver='com.mysql.jdbc.Driver',
          dbtable=table_name,
          user='root',
          password='varunbwaj').mode('append').save()

# Write raw data to MySQL
query1 = combined_df.writeStream.outputMode("append").foreachBatch(lambda df, epoch_id: write_to_mysql(df, epoch_id, 'reddit_data')).start()

# Write aggregated data to MySQL
query2 = aggregated_df.writeStream.outputMode("complete").foreachBatch(lambda df, epoch_id: write_to_mysql(df, epoch_id, 'reddit_avg_score')).start()

query1.awaitTermination()
query2.awaitTermination()