## Q4
# Stations with 25-30 degrees maximum temperature and 100-200mm maximum percipitation

from pyspark import SparkContext

sc = SparkContext(appName = "Lab 1")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = (year,temperature)
station_temp = lines.map(lambda x: (x[0], float(x[3])))
max_temp = station_temp.reduceByKey(max)
filtered_max_temp = max_temp.filter(lambda x: x[1]>=25 and x[1]<=30)

percipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
# (station, year-month-day, time, percipitation, quality)
perc_lines = percipitation_file.map(lambda line: line.split(";"))

# (station, percipitation)
station_perc = perc_lines.map(lambda x: ((x[0], x[1]), (float(x[3]))))
perc_sum = station_perc.reduceByKey(lambda a,b: (a+b))
# Maximum percipitation between 100 and 200mm
filtered_max_perc = perc_sum.filter(lambda x: x[1]>=100 and x[1]<=200).map(lambda x: (x[0][0], x[1]))

result = filtered_max_temp.join(filtered_max_perc)
result = result.repartition(1)

result.saveAsTextFile("BDA/output/Lab_1/A4")









