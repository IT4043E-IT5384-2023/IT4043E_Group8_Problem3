import logging
import json
import queue
import threading
import os

import tweepy
from kafka import KafkaProducer, KafkaConsumer

KAFKA_BOOTSTRAP_SERVER = os.environ.get('KAFKA_BOOTSTRAP_SERVER')
TW_BEARER = os.environ.get('TWITTER_BEARER_KEY')


logging.basicConfig(
    filename='app.log',
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

def get_kafka_producer():
    kafka_producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    print('Kafka producer is initiated.')
    logging.info('Kafka producer is initiated.')
    return kafka_producer

# min_faves: 100
# min_retweet: 10
# pages: 30
# wait_time: 30
class StreamListener(tweepy.StreamingClient):
    def __init__(self, kafka_topic, kafka_producer, batch_size, wait_on_rate_limit):
        super().__init__(bearer_token=TW_BEARER, wait_on_rate_limit=wait_on_rate_limit)
        self.topic = kafka_topic
        self.producer = kafka_producer
        self.batch_size = batch_size
        self.tweet_queue = queue.Queue(maxsize=batch_size)  # Queue object for storing tweets
        self.tweet_ids = []  # Empty list for storing tweet ids


    def on_connect(self):
        # Display connect message upon establishing connection with Twitter StreamingClient
        logging.info('Twitter Streaming is connected.')
        print('Twitter Streaming is connected.')

    def on_tweet(self, tweet):
        # Get tweet id from tweet object
        tweet_id = tweet.id
		
		# Parse other relevant information from tweet object, for example,
        tweet_text = tweet.text
		
		# Create a dictionary of the parsed data such as tweet_id, tweet_text
        kafka_msg = {
			'tweet_id': tweet_id,
			'tweet_text': tweet_text,
		    }
		
        # Here we will be storing the dictionary with relevant information in a queue.
        # We will send the message in batch using queue to the Kafka topic
        # once the queue is full to avoid overloading. This process will be repeated.
        # We will also place a check to avoid storing duplicate tweets in the queue.
        while self.tweet_queue.qsize() < self.batch_size and tweet_id not in self.tweet_ids:
            self.tweet_ids.append(tweet_id)  # Insert tweet id into the tweet_ids list                
            # Put tweet in queue for processing
            self.tweet_queue.put(kafka_msg)

        # After the queue is full (specified by the batch_size), send tweets to Kafka topic.
        message_count = 0  # message counter to keep track of count of records in tweet_queue
        if self.tweet_queue.qsize() == self.batch_size:
            while not self.tweet_queue.empty():
                message_count += 1
		# Get tweet from tweet_queue.
                tweet = self.tweet_queue.get()
				
		# Send tweet to Kafka topic and display success/error message after sending.
                self.producer.send(self.topic, tweet).add_callback(self.on_send_success).add_errback(self.on_send_error)
                # Mark task as done in queue
                self.tweet_queue.task_done()
				
	    # After sending all the tweets to the topic, when queue is empty, log the status message in log file.
            if self.tweet_queue.empty():
                logging.info(f'Tweets in queue pushed to KAFKA BROKER: count = {message_count}; batch # {self.batch_count}.')
                logging.info('Tweet queue emptied.')

    def on_send_success(self, record_metadata):
        # Define a callback function to handle delivery reports
        print(f'Sent tweet data to topic: {record_metadata.topic} at partition: {record_metadata.partition} with offset: {record_metadata.offset}.')
        logging.info(f'Sent tweet data to topic: {record_metadata.topic} at partition: {record_metadata.partition} with offset: {record_metadata.offset}.')

    def on_send_error(self, excp):
        # Define a callback function to handle error reports
        print(f'Error sending message: {excp}.')
        logging.error(f'Error sending message: {excp}.')


####################
class TweetConsumer(threading.Thread):
    def __init__(self, kafka_consumer):
        super().__init__()
        self.consumer = kafka_consumer

    def run(self):
        # batch counter to keep track of the batch.
        batch_count = 1
        batch_size = 100
        while True:
            # Get batch of tweets from Kafka topic
            tweet_batch = self.consumer.poll(timeout_ms=1000, max_records=batch_size)
			
            # If tweet_batch is not empty
            if len(tweet_batch) > 0:
                print("Adding records from the broker topic to the database on Amazon cloud...")
                for tp, messages in tweet_batch.items():
                    for message in messages:
                        # Decode downloaded message (tweet data)
                        tweet = json.loads(message.value.decode('utf-8'))
                        # Call function to insert tweets into database using PostgreSQL
                        insert_tweet_data(tweet)

                print(f'Records in batch # {batch_count} added successfully to Amazon cloud DB!')
                logging.info(f'Records in batch # {batch_count} added successfully Amazon cloud DB!')
                batch_count += 1

def get_kafka_consumer():
    kafka_consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        # value_deserializer=lambda x: json.loads(x.decode('utf-8')
    )
    
    print('Kafka consumer is initiated.')
    logging.info('Kafka consumer is initiated.')
    return kafka_consumer

def insert_tweet_data(tweet):
    tweet_data = (tweet['tweet_id'], tweet['tweet_text'])
    with open("tweet_data.txt", "a") as f:
        f.write(str(tweet_data))
        f.write("\n")

def main():
    # Set up Kafka
    kafka_producer = get_kafka_producer()
    kafka_consumer = get_kafka_consumer()

    # Set up tweet listener for filtered streaming
    client = StreamListener(
        'data-science-tweets',
        kafka_producer = kafka_producer,
        batch_size = 100,
        wait_on_rate_limit=True
    )
    
    logging.info('Twitter streaming client is initiated.')
	
    # Add filtered streaming rules
    client.add_rules([tweepy.StreamRule('data science')])
    logging.info('Rules added for filtered streaming.')
	
    try:
        # Start Consumer to retrieve messages from the topic and send to database for storage
        logging.info('Starting Kafka consumer to insert tweets from topic into database...')
        consumer = TweetConsumer('data-science-tweets', kafka_consumer)
        consumer.start()

	    # Start streaming
        logging.info('Connecting to Twitter filtered streaming...')
        client.filter()

        # Close the producer instance
        kafka_producer.close()
        logging.info('Producer instance is closed.')

        # Close the consumer instance
        kafka_consumer.close()
        logging.info('Consumer instance is closed.')

    except KeyboardInterrupt as e:
        print('Stopped.', e)

if __name__ == '__main__':
    main()