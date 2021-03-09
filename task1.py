from pyspark import SparkConf, SparkContext
import sys
from pyspark.sql import SparkSession, SQLContext




conf = SparkConf().setAppName("ProjectPartTwo").setMaster('local[*]')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)
sqlContext = SQLContext(sc)
sc.setLogLevel('ERROR')



INPUT_DATA_PATH = sys.argv[1]



comments = sc.textFile(INPUT_DATA_PATH + '/comments.csv.gz')
