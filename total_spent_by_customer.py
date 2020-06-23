from pyspark import SparkConf, SparkContext
from operator import add

conf = SparkConf().setMaster("local").setAppName("CustomerOrders")
sc = SparkContext(conf = conf)

# Split each comma-delimited line into fields
def parseLine(line):
    fields = line.split(',')
    customerID = (int(fields[0]))
    amount = float(fields[2])
    return (customerID, amount)

# Map each line to key/value pairs of customer ID and dollar amount
lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
parsedLines = lines.map(parseLine)

# Use reduceByKey to add up amount spent by customer ID
total_customer = parsedLines.reduceByKey(add)
total_by_customer_sorted = total_customer.sortBy(lambda x: x[1])

# collect() the results and print them
results = total_by_customer_sorted.collect()

for result in results:
    print("Customer " + str(result[0]) + "\t{:.2f}$".format(result[1]))