import sys
from pyspark import SparkConf, SparkContext
from math import sqrt

#To run on EMR successfully + output results for Star Wars:
#aws s3 cp s3://sundog-spark/MovieSimilarities1M.py ./
#aws s3 sp c3://sundog-spark/ml-1m/movies.dat ./

# the default executor memory budget of 512MB is insuficcient for processing one million movie ratings.
#   this is why we set up --executor-memory for 1 GB
#   unfortunately, the only way to check it is by try-and-error
#spark-submit --executor-memory 1g MovieSimilarities1M.py 260


def loadMovieNames():
    movieNames = {}
    with open("ml-25m/movies.csv", encoding='ascii', errors='ignore') as f:
        for line in f:
            fields = line.split(",")
            if fields[0].isnumeric():
                movieNames[int(fields[0])] = fields[1]
    return movieNames

def makePairs(userRatings):
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return ((movie1, movie2), (rating1, rating2))

def filterDuplicates(userRatings):
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    print("\nmovie1:")
    print(movie1)
    print("\nmovie2:")
    print(movie2)
    return movie1 < movie2

def computeCosineSimilarity(ratingPairs):
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))

    return (score, numPairs)

def computeRatingsData(userRatings):
    if userRatings[0].isnumeric():
        return (int(userRatings[0]), (int(userRatings[1]), float(userRatings[2])))

conf = SparkConf()
sc = SparkContext(conf = conf)

print("\nLoading movie names...")
nameDict = loadMovieNames()

data = sc.textFile("file:///SparkCourse/ml-25m/ratings.csv")
# data => (userId,movieId,rating,timestamp)

# Map ratings to key / value pairs: user ID => movie ID, rating
ratings = data.map(lambda l: l.split(",")).map(computeRatingsData)
# ratings => (userId,(movieId,rating))

print("\nJoining ratings by user...")
# Emit every movie rated together by the same user.
# Self-join to find every combination.
ratingsPartitioned = ratings.partitionBy(100)
joinedRatings = ratingsPartitioned.join(ratingsPartitioned)

print("\nRemoving duplicates...")
# At this point our RDD consists of userID => ((movieID, rating), (movieID, rating))
# joinedRatings => userID => ((movieID, rating), (movieID, rating))

# Filter out duplicate pairs
uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

# Now key by (movie1, movie2) pairs.
moviePairs = uniqueJoinedRatings.map(makePairs).partitionBy(100)

# We now have (movie1, movie2) => (rating1, rating2)
# Now collect all ratings for each movie pair and compute similarity
print("\nCollecting ratings per movie pair...")
moviePairRatings = moviePairs.groupByKey()

# We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
# Can now compute similarities.
print("\nCalculating similarities...")
moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).persist()


# Save the results if desired
print("\nSorting results...")
#moviePairSimilarities.sortByKey()
#moviePairSimilarities.saveAsTextFile("movie-sims")

# Extract similarities for the movie we care about that are "good".
if (len(sys.argv) > 1):

    scoreThreshold = 0.97
    coOccurenceThreshold = 1000

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    
    print("\nFiltering results...")
    filteredResults = moviePairSimilarities.filter(lambda pairSim: \
        (pairSim[0][0] == movieID or pairSim[0][1] == movieID) \
        and pairSim[1][0] > scoreThreshold and pairSim[1][1] > coOccurenceThreshold)

    # Sort by quality score.
    print("\nSorting results by quality...")
    #results = filteredResults.map(lambda pairSim: (pairSim[1], pairSim[0])).sortByKey(ascending = False).take(10)
    print(filteredResults is None)
    print("Top 10 similar movies for " + nameDict[movieID])
    #for result in results:
     #   (sim, pair) = result
        # Display the similarity result that isn't the movie we're looking at
      #  similarMovieID = pair[0]
       # if (similarMovieID == movieID):
        #    similarMovieID = pair[1]
        #print(nameDict[similarMovieID] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1]))
