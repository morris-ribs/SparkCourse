from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("file:///SparkCourse/fakefriends.csv")
rdd = lines.map(parseLine)
# mapValues is going to take each value in our RDD and return the age linked to the number of friends and number 1
#   Example: 
#       (33,385) => (33,(385,1))
#       (33,2) => (33,(2,1))
#       (55,221) => (55,(221,1))
# reduceByKey will add up all values for each unique key
#   Example: for key 33 => (33, (387,2))
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
# compute average for each age
#   Example: for key 33 => 387/2 = 193.5
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()
for result in results:
    print(result)
