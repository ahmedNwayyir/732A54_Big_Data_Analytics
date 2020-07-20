## Q2_1
# Monthly readings higher than 10 degrees

from pyspark import SparkContext
sc = SparkContext(appName = "Lab_1")

temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
# (station, year-month-day, time, temperature, quality)
lines = temperature_file.map(lambda line: line.split(";"))

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