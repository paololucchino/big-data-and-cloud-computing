from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .master("local") \
    .appName("BDACC Demo") \
    .getOrCreate()

import random

NUM_SAMPLES = 1000000000000000

def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1

sc = spark.sparkContext

count = sc.parallelize(range(0, NUM_SAMPLES)) \
            .filter(inside) \
            .count()

print("Pi is roughly %f" % (4 * count / NUM_SAMPLES))
