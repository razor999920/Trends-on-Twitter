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

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests


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
dataStream = ssc.socketTextStream("twitter",9009)
# dataStream = ssc.socketTextStream("localhost",9009)

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

# split each tweet into words
words = dataStream.flatMap(lambda line: line.split(" "))

# filter the words to get only hashtags
hashtags = words.filter(lambda w: '#' in w)
# Change all hashtags to lower
hashtags = hashtags.map(lambda x : x.lower())

# filter the hashtags to get only from the hashtag list
track = ['#nba', '#NBAPlayoffs', '#SportsGambling', '#Nets', "#RallyTheValley"]
track = [x.lower() for x in track]
# Change all tracking hashtags to lower
hashtag_track = hashtags.filter(lambda h: h in track)

# map each hashtag to be a pair of (hashtag,1)
hashtag_counts = hashtag_track.map(lambda x: (x, 1))

# hashtag_counts.pprint()

# adding the count of each hashtag to its last count
def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

# do the aggregation, note that now this is a sequence of RDDs
hashtag_totals = hashtag_counts.updateStateByKey(aggregate_tags_count)

def send_df_to_dashboard(top10):
	# extract the hashtags from dataframe and convert them into array
	top_tags = [str(t[0]) for t in top10]
	# extract the counts from dataframe and convert them into array
	tags_count = [p[1] for p in top10]
	# initialize and send the data through REST API
	url = 'http://dashboard:5001/updateData'
	request_data = {'label': str(top_tags), 'data': str(tags_count)}
	response = requests.post(url, data=request_data)

# process a single time interval
def process_interval(time, rdd):
    # print a separator
    print("----------- %s -----------" % str(time))
    # try:
    # sort counts (desc) in this time instance and take top 10
    sorted_rdd = rdd.sortBy(lambda x:x[1], False)
    top10 = sorted_rdd.take(10)

    # print it nicely
    for tag in top10:
        print('{:<40} {}'.format(tag[0], tag[1]))
    
    # make connection to app.py
    send_df_to_dashboard(top10)

    # except:
    #     e = sys.exc_info()[0]
    #     print("Error: %s" % e)


# do this for every single interval
hashtag_totals.foreachRDD(process_interval)

# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()