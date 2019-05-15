from pyspark import SparkContext, SparkConf, StorageLevel

if __name__ == "__main__":
    conf = SparkConf().setAppName("persist").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    inputIntegers = [1, 2, 3, 4, 5]
    integerRdd = sc.parallelize(inputIntegers)
    
    integerRdd.persist(StorageLevel.MEMORY_ONLY)
    #integerRdd.saveAsTextFile("out/integerO")
    
    value_prod = integerRdd.reduce(lambda x, y: x*y)
    value_plus = integerRdd.reduce(lambda x, y: x+y)
    kount = integerRdd.count()
    print("count ----------------------- {}".format(kount))
    print("value ----------------------- {}".format(value_prod))
    print("value ----------------------- {}".format(value_plus))
