## Q3
# Average monthly temperature

from pyspark import SparkContext

sc = SparkContext(appName = "Lab_1")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
# (station, year-month-day, time, temperature, quality)
lines = temperature_file.map(lambda line: line.split(";"))

# ((year, month, day, station), (temperature))
station_temp = lines.map(lambda x: ((int(x[1][0:4]), int(x[1][5:7]), int(x[1][8:10]), x[0]), (float(x[3]))))
# Readings during 1960-2014
filtered_temp = station_temp.filter(lambda x: x[0][0] >= 1960 and x[0][0] <=2014)

min_temp = filtered_temp.reduceByKey(min)
max_temp = filtered_temp.reduceByKey(max)
# ((year, month, day, station), (min, max))
min_max  = min_temp.join(max_temp)

# ((year, month, station), 1)
counter = min_max.map(lambda x: ((x[0][0], x[0][1], x[0][3]), 1))
# ((year, month, station), count)
count   = counter.reduceByKey(lambda a,b: (a+b))

# ((year, month, station), (min, max))
daily_min_max = min_max.map(lambda x: ((x[0][0], x[0][1], x[0][3]), (x[1])))
# ((year, month, station), (min_sum, max_sum))
min_max_sum   = daily_min_max.reduceByKey(lambda a,b: ((a[0]+b[0]), (a[1]+b[1])))

# ((year-month, station), ((min_sum, max_sum), count))
joint_RDD = min_max_sum.join(count)

# ((year, month, station), average) where average taken as (min_sum + max_sum / count * 2)
avg_temp = joint_RDD.map(lambda x: (x[0], ((x[1][0][0]+x[1][0][1])/(x[1][1]*2))))
avg_temp = avg_temp.sortBy(ascending = False, keyfunc=lambda k: k[1]).repartition(1)

avg_temp.saveAsTextFile("BDA/output/Lab_1/A3")