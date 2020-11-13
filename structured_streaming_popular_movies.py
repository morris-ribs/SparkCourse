# -*- coding: utf-8 -*-
"""
Created on Wed Dec 18 09:15:05 2019

@author: Frank
"""

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

from pyspark.sql.functions import split

def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.ITEM") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)
# broadcast the objects to every node in the cluster
# then, gets back the object back i nthe variable nameDict
nameDict = sc.broadcast(loadMovieNames())

# Create a SparkSession (the config bit is only for Windows!)
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("StreamingPopularMovies").getOrCreate()

# Monitor the logs directory for new rating data, and read in the raw lines as accessLines
#   for development purposes we can use spark.read, and read in a static way
#   readStream sits waiting for changes in the directory - "movies" here - and responds to them
accessLines = spark.readStream.text("movies")
moviesDF = accessLines.select(split('value', '\t').getItem(0).alias('user_id'),
                         split('value', '\t').getItem(1).alias('item_id'),
                         split('value', '\t').getItem(2).cast('integer').alias('rating'),
                         split('value', '\t').getItem(3).alias('timestamp'))

query = moviesDF.writeStream.format("console").start()

# Keep a running count of every access by status code
#movieCountsDF = moviesDF.reduceByKey(lambda x, y: x + y)

#flippedDF = movieCountsDF.map( lambda x : (x[1], x[0]))
#sortedMoviesDF = flippedDF.sortByKey()
#sortedMoviesWithNamesDF = sortedMoviesDF.map(lambda countMovie : (nameDict.value[countMovie[1]], countMovie[0]))

#resultsDF = sortedMoviesWithNamesDF.collect()

# Kick off our streaming query, dumping results to the console
#query = ( resultsDF.writeStream.outputMode("complete").format("console").queryName("select_movies").start() )


# Run forever until terminated
query.awaitTermination()

# Cleanly shut down the session
spark.stop()

