from pyspark import SparkContext

sc = SparkContext(appName = "Lab_1")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
# (station, year-month-day, time, temperature, quality)
lines = temperature_file.map(lambda line: line.split(";"))

# (year,temperature)
year_temperature = lines.map(lambda x: (x[1][0:4], float(x[3])))
# Readings during 1950-2014
year_temperature = year_temperature.filter(lambda x: int(x[0])>=1950 and int(x[0])<=2014)


# take min => take max => join
min_temperatures = year_temperature.reduceByKey(min)
max_temperatures = year_temperature.reduceByKey(max)
min_max = max_temperatures.join(min_temperatures)

# sort => combine in 1 file
min_max = min_max.sortBy(ascending = False, keyfunc=lambda k: k[1][0])
min_max = min_max.repartition(1)

min_max.saveAsTextFile("BDA/output/Lab_1/A1")