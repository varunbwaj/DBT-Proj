import praw
from kafka import KafkaProducer
import json
import time
import datetime

# Reddit API credentials
reddit = praw.Reddit(client_id='7rae84HhPjnd9P1382BH9A',
                     client_secret='bXdbWNh__ibW6CmVvsSgbnynUk_h-w',
                     user_agent='dbtAssignmentv1')

# Kafka Producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Fetch data from Reddit and send it to Kafka

start_time = time.time()
end_time = start_time + 15 * 60  # 15 minutes later

seen_posts = set()  # To keep track of posts we've already seen

iteration = 1  # To keep track of the current iteration
counter = 1

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