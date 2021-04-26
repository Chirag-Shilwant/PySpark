from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import os
import sys
import numpy

inputFile = None
outputFile = None

def writeOutput(content):
    global outputFile
    content = content.toPandas().to_numpy()
    numpy.savetxt(outputFile+".txt", content, fmt='%s')


def main():
    noOfArg = len(sys.argv)
    if noOfArg != 4:
	    print("Incorrect number of arguments")
	    sys.exit(0)

    global inputFile
    spark = SparkSession.builder.appName("groupbyagg").getOrCreate()
    global outputFile
    cpu, inputFile, outputFile = int(sys.argv[1]), str(sys.argv[2]), str(sys.argv[3])

    if cpu < 1:
	    print("Partitions must be greater that 0")
	    sys.exit(0)

    df = spark.read.csv(inputFile, inferSchema=True, header=True)  
    newDf = df.repartition(cpu)
    
    content = newDf.groupBy('COUNTRY').count()
    content = content.sort(content["count"].desc()).limit(1)

    writeOutput(content)

main()