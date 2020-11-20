from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

from pyspark.sql.functions import split, desc

def parseLine(line):
    fields = line.split('|')
    movieId = int(fields[0])
    movieName = fields[1]
    return (movieId, movieName)

def parseDataLine(line):
    fields = line.split('\t')
    user_id = int(fields[0])
    item_id = int(fields[1])
    rating = int(fields[2])
    timestamp = fields[3]
    return (user_id, item_id, rating, timestamp)

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)
# broadcast the objects to every node in the cluster
# then, gets back the object back in the variable nameDict
#nameDict = sc.broadcast(loadMovieNames())
lines = sc.textFile("ml-100k/u.ITEM")
spark = SparkSession(sc)
rdd = lines.map(parseLine)
moviesCollection = rdd.toDF(("MovieId", "MovieName"))

# Create a SparkSession (the config bit is only for Windows!)
#spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("StreamingPopularMovies").getOrCreate()

dataLines = sc.textFile("ml-100k/u.data")
# Monitor the logs directory for new rating data, and read in the raw lines as accessLines
#   for development purposes we can use spark.read, and read in a static way
#   readStream sits waiting for changes in the directory - "movies" here - and responds to them
#accessLines = spark.readStream.text("movies")
#moviesDF = accessLines.select(split('value', '\t').getItem(0).alias('user_id'),
#                        split('value', '\t').getItem(1).alias('item_id'),
#                        split('value', '\t').getItem(2).cast('integer').alias('rating'),
#                        split('value', '\t').getItem(3).alias('timestamp'))

moviesDF = dataLines.map(parseDataLine).toDF(('user_id', 'item_id', 'rating', 'timestamp'))

movieDFCounts = moviesDF.groupBy(moviesDF.item_id).count()
movieDFCountsLimited = movieDFCounts.sort(desc("count")).limit(10)

cond = [movieDFCountsLimited.item_id == moviesCollection.MovieId]
movieDFFinal = movieDFCountsLimited.join(moviesCollection, cond).select('MovieName','count').sort(desc("count"))

movieDFFinal.show()
#query = ( movieDFFinal.writeStream.outputMode("complete").format("console").queryName("counts").start() )

# Run forever until terminated
#query.awaitTermination()

# Cleanly shut down the session
#spark.stop()

