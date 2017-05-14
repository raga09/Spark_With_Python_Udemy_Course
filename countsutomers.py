from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomersOrdersTotals")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    cutomerId = int(fields[0])
    amountSpent = float(fields[2])
    return (cutomerId, amountSpent)

lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
rdd = lines.map(parseLine)
totalsByAmount = rdd.reduceByKey(lambda x, y: x + y)



totalsByAmountSorted = totalsByAmount.map(lambda x: (x[1], x[0])).sortByKey()

results = totalsByAmountSorted.collect()
for result in results:
    totalAmount = result[0]
    custIDs = result[1]
    print( "%i %f" % (custIDs, totalAmount))
   
    
    #\t{:.2f}F
