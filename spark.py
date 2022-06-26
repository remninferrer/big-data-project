import json

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.sql.types import *
from geopy.geocoders import Nominatim
from textblob import TextBlob


TCP_IP = 'localhost'
TCP_PORT = 9001


def processTweet(tweet):

    # (i) Sentiment analysis,
    # (ii) Get data corresponding to place where the tweet was generate (using geopy or googlemaps)
    # (iii) Index the data 


    tweetData = tweet.split("::")

    if len(tweetData) > 1:
        
        text = tweetData[1]
        rawLocation = tweetData[0]

    # (i) Apply Sentiment analysis in "text"

        tweetsentiment = TextBlob(text)
        if float(tweetsentiment.sentiment.polarity) > 0.3:
                tweetpositivity = "Positive"
        elif float(tweetsentiment.sentiment.polarity) < -0.3:
                tweetpositivity = "Negative"
        else:
                tweetpositivity = "Neutral"

    # (ii) Get geolocation (state, country, lat, lon, etc...) from rawLocation

        print("\n\n=========================\ntweet: ", tweet)
        print("Raw location from tweet status: ", rawLocation)
        
        geolocator = Nominatim(user_agent="agent1")
        location = geolocator.geocode(rawLocation)                
        
        lat = lon = state = country = None
        try:
            lat = location.latitude
        except:
            lat = None
        try:
            lon = location.longitude
        except:
            lon = None
        try:
            reverselocation = geolocator.reverse((lat, lon))
            state = reverselocation.raw['address']['state']
        except:
            state = None
        try:
            reverselocation = geolocator.reverse((lat, lon))
            country = reverselocation.raw['address']['country']
        except:
            country = None

        print("lat: ", lat)
        print("lon: ", lon)
        print("state: ", state)
        print("country: ", country)
        print("Text: ", text)
        print("Sentiment: ", tweetpositivity)
        
        # remove quotation marks for json
        tweetpositivity = tweetpositivity.replace('"','\\"')
        
        # append to json
        fileOpen = open('data.json', 'a')
        fileOpen.write('{"lat":"'+str(lat)+'","lon":"'+str(lon)+'","state":"'+str(state) 
        +'","country":"'+str(country) +'","text":"'+str(text)+'","sentiment":"'+str(tweetpositivity)+'"}\n')
		
        
# (iii) Log data and print table 
# Utilized databricks to print table     

# Pyspark
# create spark configuration
conf = SparkConf()
conf.setAppName('TwitterApp')
conf.setMaster('local[2]')

# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# create the Streaming Context from spark context with interval size 4 seconds
ssc = StreamingContext(sc, 4)
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 9001
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)

dataStream.foreachRDD(lambda rdd: rdd.foreach(processTweet))

ssc.start()
ssc.awaitTermination()
