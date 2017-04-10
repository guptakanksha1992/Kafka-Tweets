#!/usr/bin/env python
"""
   Import modules
"""
import tweepy
import json
from kafka import KafkaProducer


#loading credentials and defining the Topic used by Kafka queue.

KAFKA_HOST = ''
TOPIC = ''
twitter_consumer_key=''
twitter_consumer_secret =''
twitter_access_token =''
twitter_access_secret =''


#Twitter authentication: to fetch tweets

twitter_auth = tweepy.OAuthHandler(twitter_consumer_key, twitter_consumer_secret)
twitter_auth.set_access_token(twitter_access_token, twitter_access_secret)

#Twitter API object
twitter_api = tweepy.API(twitter_auth, wait_on_rate_limit_notify=True, retry_count=3, retry_delay=56

class StreamListener(tweepy.StreamListener):
    """
        Stream Listener
    """

    producer = KafkaProducer(bootstrap_servers=[KAFKA_HOST])
    def on_data(self, data):
        """
            On proper status
        """
        try:
            json_data = json.loads(data)
            if (json_data is not None and
                    json_data['text'] is not None):

                # Convert unicode to string
                tweet = str(json_data['text'])
                print type(tweet)
                print tweet
                future = self.producer.send(TOPIC, tweet)


        except (KeyError, UnicodeDecodeError, Exception) as e:
            pass

    def on_error(self, status_code):
        """
            handle error of listener
        """
        if status_code == 420:
            print "YOU ARE BEING RATE LIMITED"
            return True  #Do not disconnect stream

def main():
    """
        main method of script
    """
    print "Streaming Tweets and sending to Kafka..."
    stream_listener = StreamListener()
    while True:
        try:
            streamer = tweepy.Stream(twitter_api.auth,
                listener=stream_listener)
            streamer.filter(locations=[-180, -90, 180, 90], languages=['en'])
        except Exception as e:
            print "Error inserting tweet to Kafka!!"

if __name__ == '__main__':
    main()
