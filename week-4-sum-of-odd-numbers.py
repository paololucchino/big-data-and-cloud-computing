from pyspark import SparkConf, SparkContext
from math import sqrt

conf = SparkConf().setAppName("OddNumberSums")
sc = SparkContext(conf=conf)

n = 50

allNumbers = sc.parallelize(range(n*2))
allNumbers.collect()

keyByOddEven = allNumbers.map(lambda x: (x%2, x))
keyByOddEven.collect()

sumNumbers = keyByOddEven.reduceByKey(lambda x, y: x+y)
sumNumbers.collect()

sums = sumNumbers.collect()

print('The sum of the first {} odd numbers is {}, which is the square of {}'.format(n,
                                                                                   sums[1][1],
                                                                                   sqrt(sums[1][1])))

sc.cancelAllJobs()
sc.stop()