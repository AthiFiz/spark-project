from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerSpending")
sc = SparkContext(conf=conf)

def splitLine(line):
    fields = line.split(",")
    customerID = int(fields[0])
    spending = float(fields[2])
    return customerID, spending

lines = sc.textFile("./data/customer-orders.csv")

rdd = lines.map(splitLine)
purchase = rdd.reduceByKey(lambda x,y: x+y)
sorted = purchase.map(lambda x: (x[1], x[0])).sortByKey()
results = sorted.collect()

for purchase, id in results:
    print(f"{id}:   {purchase:.2f}/-")