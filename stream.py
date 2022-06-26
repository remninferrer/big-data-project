import tweepy
import socket
import re
import preprocessor # used in preprocessing tweets

# Get tokens/keys
# ADD YOUR KEYS HERE when running this code, but remove it before pushing your update to github
ACCESS_TOKEN = ''
ACCESS_SECRET = ''
CONSUMER_KEY = ''
CONSUMER_SECRET = ''

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

hashtag = '#covid19'

TCP_IP = 'localhost'
TCP_PORT = 9001


def preprocessing(tweet):
    
    # Add here your code to preprocess the tweets and  
    # remove Emoji patterns, emoticons, symbols & pictographs, transport & map symbols, flags (iOS), etc
    pattern = re.compile("["
                        u"\U0001F600-\U0001F64F"
                        u"\U0001F300-\U0001F5FF"
                        u"\U0001F680-\U0001F6FF"
                        u"\U0001F1E0-\U0001F1FF"
                        u"\U00002700-\U00002702"
                        u"\U000024C2-\U0001F251"
                        "]+", flags=re.UNICODE)
    

    tweet = re.sub(r'[^\x00-\x7F]+',' ', tweet)
    
    tweet = pattern.sub(r'', tweet)
    
    return preprocessor.clean(tweet)
    # end of preprocessing


def getTweet(status):
    
    # The status objects has additional data other than tweet and location, but only those two are used for this project
    
    tweet = ""
    location = ""

    location = status.user.location
    
    if hasattr(status, "retweeted_status"):  # Check if Retweet
        try:
            tweet = status.retweeted_status.extended_tweet["full_text"]
        except AttributeError:
            tweet = status.retweeted_status.text
    else:
        try:
            tweet = status.extended_tweet["full_text"]
        except AttributeError:
            tweet = status.text

    return location, preprocessing(tweet)

# create sockets
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
conn, addr = s.accept()

class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        location, tweet = getTweet(status)

        if (location != None and tweet != None):
            tweetLocation = location + "::" + tweet+"\n"
            print(status.text)
            conn.send(tweetLocation.encode('utf-8'))

        return True


    def on_error(self, status_code):
        if status_code == 420:
            return False
        else:
            print(status_code)

myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())
myStream.filter(track=[hashtag], languages=["en"])

