"""API ACCESS KEYS"""

access_token = "716707878963437568-ixoiIjNZoB1sonJcuEyLN54lbvJhzfU"
access_token_secret = "M3WzStqcoYroGwMMxOAQzixIuVaCxJlloYbZUBXOQaDcl"
consumer_key = "PGFgjXVjjxBm12LKSQuh0svpw"
consumer_secret = "p79muCs178jRExysGoOMGa8C9YaYqISXbC06LGLEYBJDvJORyr"


from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092') #Same port as your Kafka server


topic_name = "twitter-stream"


class twitterAuth():
    """SET UP TWITTER AUTHENTICATION"""

    def authenticateTwitterApp(self):
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        return auth



class TwitterStreamer():

    """SET UP STREAMER"""
    def __init__(self):
        self.twitterAuth = twitterAuth()

    def stream_tweets(self):
        while True:
            listener = ListenerTS() 
            auth = self.twitterAuth.authenticateTwitterApp()
            stream = Stream(auth, listener)
            stream.filter(track=["Pedro Castillo"], stall_warnings=True, languages= ["es"])


class ListenerTS(StreamListener):

    def on_data(self, raw_data):
            producer.send(topic_name, str.encode(raw_data))
            return True


if __name__ == "__main__":
    TS = TwitterStreamer()
    TS.stream_tweets()