import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    '''
    Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
    print the sum of those numbers to console.
    Each row of the input file contains 10 prime numbers separated by spaces.
    '''
    conf = SparkConf().setAppName('prime numbers sum').setMaster('local[*]')
    context = SparkContext(conf = conf)
    rdd = context.textFile('in/prime_nums.text')
    nums_rdd = rdd.flatMap(lambda line: line.split('\t'))
    rdd100 = nums_rdd.map(lambda x: int(x)).collect()
    rdd80 = nums_rdd.map(lambda x: int(x)).take(80)
    sum100 = sum(rdd100)
    print("sum 100 is {}".format(sum100))
    print("sum 80 is {}".format(sum(rdd80)))
