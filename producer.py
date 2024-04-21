import praw
from kafka import KafkaProducer
import json
import time

# Reddit API credentials
reddit = praw.Reddit(client_id='7rae84HhPjnd9P1382BH9A',
                     client_secret='bXdbWNh__ibW6CmVvsSgbnynUk_h-w',
                     user_agent='dbtAssignmentv1')

# Kafka Producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Fetch data from Reddit and send it to Kafka

start_time = time.time()
while True:
    for subreddit, topic in [('announcements', 'reddit_tweets_1'), ('funny', 'reddit_tweets_2'), ('worldnews', 'reddit_tweets_3')]:
        ## for submission in reddit.subreddit(subreddit).hot(limit=10):
        for submission in reddit.subreddit(subreddit).new(limit=10):
            tweet_data = {"text": submission.title, "subreddit": str(submission.subreddit), "score": submission.score, "num_comments": submission.num_comments, "created_utc": submission.created_utc}
            producer.send(topic, value=tweet_data)
    print("data sent to kafka")
    time.sleep(15)

    # Check if 60 seconds have passed
    if time.time() - start_time > 60:
        break

# Close the producer
producer.close()
