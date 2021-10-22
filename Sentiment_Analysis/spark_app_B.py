"""
    This Spark app connects to a script running on another (Docker) machine
    on port 9009 that provides a stream of raw tweets text. That stream is
    meant to be read and processed here, where top trending hashtags are
    identified. Both apps are designed to be run in Docker containers.

    To execute this in a Docker container, do:
    
        docker run -it -v $PWD:/app --link twitter:twitter eecsyorku/eecs4415

    and inside the docker:

        spark-submit spark_app.py

    For more instructions on how to run, refer to final tutorial 8 slides.

    Made for: EECS 4415 - Big Data Systems (York University EECS dept.)
    Modified by: Tilemachos Pechlivanoglou
    Based on: https://www.toptal.com/apache/apache-spark-streaming-twitter
    Original author: Hanee' Medhat  
"""
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SQLContext
import sys
import requests
import re
from nltk.sentiment.vader import SentimentIntensityAnalyzer
# from nltk.tokenize import word_tokenize
import math

# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# create the Streaming Context from spark context, interval size 2 seconds
ssc = StreamingContext(sc, 2)
# setting a checkpoint for RDD recovery (necessary for updateStateByKey)
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009
dataStream = ssc.socketTextStream("twitter", 9009)

# reminder - lambda functions are just anonymous functions in one line:
#
#   words.flatMap(lambda line: line.split(" "))
#
# is exactly equivalent to
#
#    def space_split(line):
#        return line.split(" ")
#
#    words.filter(space_split)

# Utility function to clean tweet text by removing links, special characters using simple regex statements.
def clean_tweet(tweet):
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

def filter_tweets(tweet):
    track = ['#NBA', '#RallyTheValley', '#Nets', '#ClipperNation', '#BelieveAtlanta', '#MileHighBasketball', '#Kawhi', '#Embiid', '#Young', '#Durant',
        '#Technology', '#SpaceX' '#Tesla', '#DataScience', '#NASA', '#climatechange', '#tech', '#bitcoin', '#blockchain', '#amd',
        '#Footbal', '#EUROS2020', '#UEFA', '#CopaAmerica', '#ChampionsLeague', '#UEL', '#Eriksen', '#Euro2021', '#UCL', '#Ronaldo',
        '#News', '#Covid19', '#FreePalestine', '#Gaza', '#coronavirus', '#consumers', '#foryou', '#news', '#cnn', '#bbc',
        '#Music', '#HipHop', '#Rock', '#Rap', '#Punk', '#R&B', '#Drake', '#KendrickLamar', '#Stormzy', '#ToryLanez'] 

    track = (x.lower() for x in track)

    if any(hashtag in tweet for hashtag in track):
        return True
    return False

tweet_stream =  dataStream.filter(lambda line : filter_tweets(line.lower()))


def getSentiment(line):
    nba = ['#NBA', '#RallyTheValley', '#Nets', '#ClipperNation', '#BelieveAtlanta', '#MileHighBasketball', '#Kawhi', '#Embiid', '#Young', '#Durant']
    science = ['#Technology', '#SpaceX' '#Tesla', '#DataScience', '#NASA', '#climatechange', '#tech', '#bitcoin', '#blockchain', '#amd']
    football = ['#Footbal', '#EUROS2020', '#UEFA', '#CopaAmerica', '#ChampionsLeague', '#UEL', '#Eriksen', '#Euro2021', '#UCL', '#Ronaldo']
    news = ['#News', '#Covid19', '#FreePalestine', '#Gaza', '#coronavirus', '#consumers', '#foryou', '#news', '#cnn', '#bbc']
    music = ['#Music', '#HipHop', '#Rock', '#Rap', '#Punk', '#R&B', '#Drake', '#KendrickLamar', '#Stormzy', '#ToryLanez']

    nba = (x.lower() for x in nba)
    science = (x.lower() for x in science)
    football = (x.lower() for x in football)
    news = (x.lower() for x in news)
    music = (x.lower() for x in music)

    # Lower hashtags and line
    line = line.lower()
    topic = 0
    tweet = None

    if any(topics in line for topics in nba):
        topic = 'NBA'
        tweet = line
    elif any(topics in line for topics in science):
        topic = 'Technology'
        tweet = line
    elif any(topics in line for topics in football):
        topic = 'Footbal'
        tweet = line
    elif any(topics in line for topics in news):
        topic = 'News'
        tweet = line
    elif any(topics in line for topics in music):
        topic = 'Music'
        tweet = line

    sentiment_dic = {}
    # Find the analysis on the tweet
    if (tweet is not None):
        tweet = clean_tweet(tweet)
        # print(str(topic) + ', ' + tweet)
        sid = SentimentIntensityAnalyzer()
        sentiment_dic = sid.polarity_scores(tweet)

        polarity = None

        if sentiment_dic['compound'] > 0.2:
            polarity = "positive"
        elif sentiment_dic['compound'] < -0.2:
            polarity = "negative"
        else:
            polarity = "neutral"

    return ((topic, polarity), 1)

# Sentiment the tweets using the function
sentimented_tweets = tweet_stream.map(lambda line: getSentiment(line)) 
# sentimented_tweets.pprint()           

# adding the count of each hashtag to its last count
def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

# do the aggregation, note that now this is a sequence of RDDs
tweet_reduced = sentimented_tweets.updateStateByKey(aggregate_tags_count)

def send_df_to_dashboard(topic_average):
	# extract the keys from dict and convert them into array
	top_tags = [str(str(t[0]) + '-' + t[1]) for t in topic_average.keys()]
    
	# extract the counts from dataframe and convert them into array
	tags_count = [topic_average[p] for p in topic_average.keys()]
	# initialize and send the data through REST API
	url = 'http://dashboard:5001/updateData'
	request_data = {'label': str(top_tags), 'data': str(tags_count)}
	response = requests.post(url, data=request_data)

# Topic analysis
topic_analysis = {}
total_sum = {}

# process a single time interval
def process_interval(time, rdd):
    # print a separator
    print("----------- %s -----------" % str(time))
    sentiment = rdd.take(15)

    for tag in sentiment:
        topic_analysis[tag[0]] = tag[1]
        print('{:<40} {}'.format(tag[0], tag[1]))

    # for tag in sentiment:
    #     if len(topic_analysis.keys()) == 0 or tag[0][0] not in topic_analysis.keys():
    #         if tag[0][1] == 'negative':              
    #             topic_analysis[tag[0][0]] = (tag[1] * (-1))
    #         elif tag[0][1] == 'positive':
    #             topic_analysis[tag[0][0]] = tag[1]
    #     elif tag[0][0] in topic_analysis.keys() and ((tag[1] != topic_analysis[tag[0][0]]) or (tag[1] * (-1)) != topic_analysis[tag[0][0]]):
    #         if tag[0][1] == 'negative':
    #             print("negative " + str(topic_analysis[tag[0][0]]))
    #             max_value = math.copysign(max((tag[1] * (-1)), topic_analysis[tag[0][0]]) + min((tag[1] * (-1)), topic_analysis[tag[0][0]]), -1)
    #             if max == -0.0:
    #                 max = math.copysign(max, 1)
    #             topic_analysis[tag[0][0]] = max_value
    #         elif tag[0][1] == 'positive':
    #             print("positive " + str(topic_analysis[tag[0][0]]))
    #             topic_analysis[tag[0][0]] = tag[1]
        
    #     if (tag[0][0] not in total_sum.keys()):
    #         total_sum[tag[0][0]] = 0

    # for tag in topic_analysis.keys():
    #     # Choose each topic with tag[0]
    #     # Loop throuhg topic_analysis and sum all values for same topics
    #     for key in topic_analysis.keys():
    #         if tag == key:
    #             print(str(key) + " " + str(topic_analysis[key]))
    #             total_sum[key] += topic_analysis[key]

    # print(topic_analysis)
    send_df_to_dashboard(topic_analysis)

# do this for every single interval
tweet_reduced.foreachRDD(process_interval)

# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()
