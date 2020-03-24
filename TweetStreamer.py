import tweepy
import socket
import json
import os

from tweepy.auth import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

# Credentials from https://apps.twitter.com
access_token = os.environ.get('TWITTER_ACCESS')
access_secret = os.environ.get('TWITTER_ACCESS_SECRET')
consumer_key = os.environ.get('TWITTER_CONSUMER')
consumer_secret = os.environ.get('TWITTER_CONSUMER_SECRET')

# TweetStreamListener inherits StreamListener with on_data and if_error methods
class TweetStreamListener(StreamListener):
    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            message = json.loads( data )
            print( message['text'].encode('utf-8') )
            self.client_socket.send( message['text'].encode('utf-8') )
            return True

        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def if_error(self, status):
        print(status)
        return True


def send_tweets(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    
    twitter_stream = Stream(auth, TweetStreamListener(c_socket))
    twitter_stream.filter(track=['Corona'])


server_socket = socket.socket()
server_host = "localhost"
server_port = 5555
server_socket.bind((server_host, server_port))
print("Listening on port: %s" % str(server_port))

server_socket.listen(5)
c, addr = server_socket.accept()

print("Received request from: " + str(addr))
send_tweets(c)