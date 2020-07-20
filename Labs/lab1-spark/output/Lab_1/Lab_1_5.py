## Q5
# Average monthly precipitation for the Östergotland region for the period 1993-2016

from pyspark import SparkContext
sc = SparkContext(appName = "Lab_1")

stations_file = sc.textFile("BDA/input/stations-Ostergotland.csv")
station_lines = stations_file.map(lambda line: line.split(";"))
# (stations)
stations = station_lines.map(lambda x: x[0]).collect()

percipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
# (station, year-month-day, time, percipitation, quality)
perc_lines = percipitation_file.map(lambda line: line.split(";"))

# ((station, year-month), (percipitation))
prec_rdd = perc_lines.map(lambda x: ((x[0], x[1][0:7]), (float(x[3]))))
precByStation = prec_rdd.filter(lambda x: x[0][0] in stations and int(x[0][1][0:4])>=1993 and int(x[0][1][0:4])<=2016)

# ((year-month), (percipitation, 1))
precByStation = precByStation.map(lambda x: ((x[0][1]), (x[1], 1)))
# ((year-month), (perc_sum, count))
precByStation = precByStation.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))
# (year-month, average)
precByStation = precByStation.map(lambda x: (x[0], x[1][0]/x[1][1])).repartition(1)

precByStation.saveAsTextFile("BDA/output/Lab_1/A5")
