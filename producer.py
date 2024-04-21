# import praw
# from kafka import KafkaProducer
# import json
# import time

# # Reddit API credentials
# reddit = praw.Reddit(client_id='7rae84HhPjnd9P1382BH9A',
#                      client_secret='bXdbWNh__ibW6CmVvsSgbnynUk_h-w',
#                      user_agent='dbtAssignmentv1')

# # Kafka Producer
# producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
#                          value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# # Fetch data from Reddit and send it to Kafka
# while True:
#     for submission in reddit.subreddit('formula1').hot(limit=10):
#         tweet_data = {"text": submission.title, "subreddit": str(submission.subreddit), "score": submission.score, "num_comments": submission.num_comments}
#         producer.send('reddit_tweets', value=tweet_data)
#     time.sleep(5)  
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
while True:
    for submission in reddit.subreddit('formula1').hot(limit=10):
        tweet_data = {"text": submission.title, "subreddit": str(submission.subreddit), "score": submission.score, "num_comments": submission.num_comments}
        producer.send('reddit_tweets_1', value=tweet_data)
        producer.send('reddit_tweets_2', value=tweet_data)
    time.sleep(5)