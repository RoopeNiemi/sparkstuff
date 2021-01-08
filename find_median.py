from pyspark import SparkContext, SparkConf
import math

# Find median from an array of unsorted floats, without sorting the whole array. Tested with 1 billion floats with values between 0-100.

conf = (SparkConf()
        .setAppName("local")
        .set("spark.cores.max", "10")
        .set("spark.rdd.compress", "true")
        .set("spark.broadcast.compress", "true"))
sc = SparkContext(conf=conf)

def find_median(dataset):
    halfway = math.floor(dataset.count() / 2)
    dataset = dataset.map(float)
    # Sort key-value pairs representing int-values of the floats of the dataset, and how many each int-values there are.
    totals = sorted(dataset.map(lambda x: (math.floor(x), 1)).groupByKey().mapValues(len).collect())
    # Find which part contains the median
    total = 0
    ind = 0
    for i, t in totals:
        total += t
        if total > halfway:
            ind = i
            break
        lower = total

    # Sort the part in the original dataset where the median is located
    median_part = sorted(dataset.filter(lambda x: math.floor(x) == ind).collect())
    # relative position of middlepoint in array
    m = int(halfway - lower)
    # If an even numbered array, median is the average of the two middle numbers
    if(dataset.count() % 2 == 0):
        return (median_part[m] + median_part[m - 1]) / 2
    return median_part[m]

dataset = sc.textFile("path/to/file.txt")

print("Median:", find_median(dataset))