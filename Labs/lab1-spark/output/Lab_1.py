from pyspark import SparkContext

sc = SparkContext(appName = "Lab 1")
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
##################################################################################

## Q2_1
# Monthly readings higher than 10 degrees

# (year-month, (station, temperature))
monthly_temp = lines.map(lambda x: (x[1][0:7], (x[0], float(x[3]))))
# Readings during 1950-2014 & > 10
filtered_mon_temp = monthly_temp.filter(lambda x: int(x[0][0:4]) >= 1950 and int(x[0][0:4]) <=2014 and float(x[1][1]) > 10)

# ((year-month), 1) => ((year-month), count) => combine => sort
counter = filtered_mon_temp.map(lambda x: (x[0], 1))
m_count = counter.reduceByKey(lambda x,y: x+y)
m_count = m_count.repartition(1) 
m_count = m_count.sortBy(ascending = False, keyfunc=lambda k: k[1])

m_count.saveAsTextFile("BDA/output/Lab_1/A2_1")
##################################################

## Q2_2
# Distinct readings by station per month higher than 10 degrees

# ((year-month),(station,1)) and only taking one reading per month per station
unique_counter = monthly_temp.map(lambda x: (x[0],(x[1][0],1))).distinct() 

# ((year-month), (station, count))
unique_count = unique_counter.reduceByKey(lambda x,y: (x[0], (x[1] + y[1]))) 

# map to ((year-month), (count)) => combine => sort
unique_count = unique_count.map(lambda x: (x[0], x[1][1]))
unique_count = unique_count.repartition(1) 
unique_count = unique_count.sortBy(ascending = False, keyfunc=lambda k: k[1])

unique_count.saveAsTextFile("BDA/output/Lab_1/A2_2")
##################################################################################

## Q3
# Average monthly temperature

# ((year, month, day, station), (temperature))
#station_temp = lines.map(lambda x: ((int(x[1][0:4]), int(x[1][5:7]), int(x[1][8:10]), x[0]), (float(x[3]))))
# Readings during 1960-2014
#station_temp = station_temp.filter(lambda x: x[0][0] >= 1960 and x[0][0] <=2014)

#min_temp = station_temp.reduceByKey(min)
#max_temp = station_temp.reduceByKey(max)
# ((year, month, day, station), (min, max))
#min_max  = min_temp.join(max_temp)

# ((year, month, station), 1)
#counter = min_max.map(lambda x: ((x[0][0], x[0][1], x[0][3]), 1))
# ((year, month, station), count)
#count   = counter.reduceByKey(lambda a,b: (a+b))

# ((year, month, station), (min, max))
#daily_min_max = min_max.map(lambda x: ((x[0][0], x[0][1], x[0][3]), (x[1])))
# ((year, month, station), (min_sum, max_sum))
#min_max_sum   = daily_min_max.reduceByKey(lambda a,b: ((a[0]+b[0]), (a[1]+b[1])))

# ((year-month, station), ((min_sum, max_sum), count))
#joint_RDD = min_max_sum.join(count)

# ((year, month, station), average) where average taken as (min_sum + max_sum / count * 2)
#avg_temp = joint_RDD.map(lambda x: (x[0], ((x[1][0][0]+x[1][0][1])/(x[1][1]*2))))
#avg_temp = avg_temp.sortBy(ascending = False, keyfunc=lambda k: k[1]).repartition(1)

#avg_temp.saveAsTextFile("BDA/output/Lab_1/A3")
##################################################################################
## Q4
# Stations with 25-30 degrees maximum temperature and 100-200mm maximum percipitation

#percipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
# (station, year-month-day, time, percipitation, quality)
#perc_lines = percipitation_file.map(lambda line: line.split(";"))

# (key, value) = (year,temperature)
#station_temp = lines.map(lambda x: (x[0], float(x[3])))
#day_max_temp = station_temp.reduceByKey(max)
# Maximum temperature between 25 and 30 degrees
#day_max_temp = day_max_temp.filter(lambda x: x[1]>=25 and x[1]<=30)

# (station, percipitation)
#station_perc = perc_lines.map(lambda x: ((x[0], x[1]), (float(x[3]))))
#perc_sum = station_perc.reduceByKey(lambda a,b: (a+b))
# Maximum percipitation between 100 and 200mm
#perc_sum = perc_sum.filter(lambda x: x[1]>=100 and x[1]<=200).map(lambda x: (x[0][0], x[1]))

#result = day_max_temp.join(perc_sum)
#result = result.repartition(1)

#result.saveAsTextFile("BDA/output/Lab_1/A4")
##################################################################################

## Q5
# Average monthly precipitation for the Östergotland region for the period 1993-2016

#stations_file = sc.textFile("BDA/input/stations-Ostergotland.csv")
#station_lines = stations_file.map(lambda line: line.split(";"))
# (stations)
#stations = station_lines.map(lambda x: x[0]).collect()

# ((station, year-month), (percipitation))
#prec_rdd = perc_lines.map(lambda x: ((x[0], x[1][0:7]), (float(x[3]))))
#precByStation = prec_rdd.filter(lambda x: x[0][0] in stations and int(x[0][1][0:4])>=1993 and int(x[0][1][0:4])<=2016)

# ((year-month), (percipitation, 1))
#precByStation = precByStation.map(lambda x: ((x[0][1]), (x[1], 1)))
# ((year-month), (perc_sum, count))
#precByStation = precByStation.reduceByKey(lambda atemperature_file = sc.textFile("BDA/input/temperature-readings.csv")
# (station, year-month-day, time, temperature, quality)
#lines = temperature_file.map(lambda line: line.split(";"))temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
# (station, year-month-day, time, temperature, quality)
#lines = temperature_file.map(lambda line: line.split(";"))temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
# (station, year-month-day, time, temperature, quality)
#lines = temperature_file.map(lambda line: line.split(";"))temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
# (station, year-month-day, time, temperature, quality)
#lines = temperature_file.map(lambda line: line.split(";"))temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
# (station, year-month-day, time, temperature, quality)
#lines = temperature_file.map(lambda line: line.split(";")),b: (a[0]+b[0], a[1]+b[1]))
# (year-month, average)
#precByStation = precByStation.map(lambda x: (x[0], x[1][0]/x[1][1])).repartition(1)

#precByStation.saveAsTextFile("BDA/output/Lab_1/A5")






