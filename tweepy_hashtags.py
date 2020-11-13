import operator
import tweepy
import re

consumer_key = 'ToQohhtofOnWH6EnfUibMCvuX'
consumer_secret = 'ipn9s1qDune40qweifYcuyQM2aCFNAh4mRlzrmrnhHAkK76pOU'
access_token = '1285238349364682752-z1h58Xcm3SRwpQZgpU2O9RY6BBptdf'
access_token_secret = 'vq0gTygFSBEaCDIK9KH4YbkvNsgAQYmCt5P1vN2T4ex2t'

auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
myStreamListener = tweepy.StreamListener()
myStream = tweepy.Stream(auth = auth, listener=tweepy.StreamListener)
api = tweepy.API(auth)
trump_hashtags = {}
print('------------------Trump tweets------------------')
i = 0
while i < 10:
    for tweet in tweepy.Cursor(api.search, q='trump').items(200):
        if re.search("#\w+", tweet.text) is not None:
            tweet_text = re.search("#\w+", tweet.text).group(0).encode(encoding='ascii',errors="xmlcharrefreplace")
            if tweet_text not in trump_hashtags:
                trump_hashtags[tweet_text] = 0
            trump_hashtags[tweet_text] += 1
    i+=1

# Sort Trump hashtags by value using itemgetter
sorted_trump_hashtags = dict(sorted(trump_hashtags.items(),
                            key=operator.itemgetter(1),
                            reverse=True))
print('Most popular Trump hashtags: ')
print(sorted_trump_hashtags)

biden_hashtags = {}
print('------------------Biden tweets------------------')
i = 0
while i < 10:
    for tweet in tweepy.Cursor(api.search, q='biden').items(200):
        if re.search("#\w+", tweet.text) is not None:
            tweet_text = re.search("#\w+", tweet.text).group(0).encode(encoding='ascii',errors="xmlcharrefreplace")
            if tweet_text not in biden_hashtags:
                biden_hashtags[tweet_text] = 0
            biden_hashtags[tweet_text] += 1
    i+=1

# Sort Biden hashtags by value using itemgetter
sorted_biden_hashtags = dict(sorted(biden_hashtags.items(),
                            key=operator.itemgetter(1),
                            reverse=True))
print('Most popular Biden hashtags: ')
print(sorted_biden_hashtags)

# NOTE: cache so that we do not exceed Twitter API limits