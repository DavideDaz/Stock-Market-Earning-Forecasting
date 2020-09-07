# Streams in tweets via Tweepy API

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

import twitter_credentials


class TwitterStreamer:
    """
    Class for streaming and processing live tweets.
    """

    def stream_tweets(self, fetched_tweets, hashtag_list):
        # save tweets in txt or json
        # handles Twitter authentication and connection to Tweepy API

        listener = StdOutListener()
        auth = OAuthHandler(
            twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET
        )

        auth.set_access_token(
            twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET
        )

        stream = Stream(auth, listener)

        stream.filter(track=companies_list)


class StdOutListener(StreamListener):
    """
    Basic listener class that prints tweets out to standard out.
    """

    def __init__(self, fetched_tweets):
        self.fetched_tweets = fetched_tweets

    # override methods
    def on_data(self, data):  # takes in StreamListener data

        try:
            print(data)
            with open(self.fetched_tweets, "a") as f:
                f.write(data)
            return True
        except BaseException as e:
            print("Error in 'on_data': %s" % str(e))
        return True

    def on_error(self, status):  # modified to print out errors
        print(status)


if __name__ == "__main__":

    # make an import of list of companies w/ stocks of interest
    hashtag_list = ["juul", "zoom", "moderna"]  # import here as list
    fetched_tweets = "fetched_tweets.json"

    twitter_streamer = TwitterStreamer()
    twitter_streamer.stream_tweets(fetched_tweets, hashtag_list)