from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalAmountSpent")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(",")
    customerId = int(fields[0])
    dollarAmount = float(fields[2])
    return (customerId, dollarAmount)

def printResults(results):
    for result in results:
        print(result)
    
lines = sc.textFile("file:///spark-course/customer-orders.csv")
amountSpent = lines.map(parseLine)
totalAmountSpent = amountSpent.reduceByKey(lambda x, y: round(x + y, 2))
totalAmountSpentSorted = totalAmountSpent.map(lambda x: (x[1], x[0])).sortByKey()
results = totalAmountSpentSorted.collect()

printResults(results)