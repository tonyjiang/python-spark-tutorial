import sys
sys.path.insert(0, '.')
from commons.Utils import Utils
from pyspark import SparkContext, SparkConf

def extract_name_city(line):
  fields = Utils.COMMA_DELIMITER.split(line)
  return "{}, {}".format(fields[1], fields[2])

if __name__ == "__main__":
    conf = SparkConf().setAppName("US Airports").setMaster("local[2]")
    sc = SparkContext(conf = conf)
    lines = sc.textFile("out/airports_in_usa1")
    airports1 = lines.map(lambda line: Utils.COMMA_DELIMITER.split(line)).map(lambda x:(x[0], x))
    lines = sc.textFile("out/airports_in_usa2")
    airports2 = lines.map(lambda line: Utils.COMMA_DELIMITER.split(line)).map(lambda x:(x[0], x))
    new_rdd = airports1.join(airports2).map(lambda x: (x[0], x[1][0], x[1][1]))
    #for airport in airports1.take(10):
    #    print(airport)
    #for airport in airports2.take(10):
    #    print(airport)
    for airport in new_rdd.take(10):
        print(airport)
