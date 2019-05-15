import sys
sys.path.insert(0, '.')
from commons.Utils import Utils
from pyspark import SparkContext, SparkConf

def extract_name_city(line):
  fields = Utils.COMMA_DELIMITER.split(line)
  return "{}, {}".format(fields[1], fields[2])

def extract_first_half(line):
  fields = Utils.COMMA_DELIMITER.split(line)
  return "{},{},{},{},{},{},{}".format(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6])

def extract_second_half(line):
  fields = Utils.COMMA_DELIMITER.split(line)
  return "{},{},{},{},{},{}".format(fields[0], fields[7], fields[8], fields[9], fields[10], fields[11])

if __name__ == "__main__":

    '''
    Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
    and output the airport's name and the city's name to out/airports_in_usa.text.

    Each row of the input file contains the following columns:
    Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
    ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

    Sample output:
    "Putnam County Airport", "Greencastle"
    "Dowagiac Municipal Airport", "Dowagiac"
    ...
    '''

    conf = SparkConf().setAppName("US Airports").setMaster("local[2]")
    sc = SparkContext(conf = conf)
    lines = sc.textFile("in/airports.text")
    us_airports = lines.filter(lambda line: Utils.COMMA_DELIMITER.split(line)[3] != '"United States"')
    output1 = us_airports.map(extract_first_half)
    output2 = us_airports.map(extract_second_half)
    output1.saveAsTextFile("out/airports_in_usa1")
    output2.saveAsTextFile("out/airports_in_usa2")
