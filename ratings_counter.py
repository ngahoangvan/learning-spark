from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)
sc.setLogLevel(logLevel="ERROR")

lines = sc.textFile("data/ml-32m/ratings.csv")
ratings = lines.map(lambda x: x.split(",")[2])

print(ratings.countByValue())