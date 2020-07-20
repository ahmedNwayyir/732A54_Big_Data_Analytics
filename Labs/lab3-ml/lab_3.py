from __future__ import division
from math import radians, cos, sin, asin, sqrt, exp
from datetime import datetime
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("lab_kernel")#.setMaster("local[*]")
sc = SparkContext.getOrCreate()

### Parameters
h_distance = 100
h_date = 30
h_time = 3
lat = 58.4274 
long = 14.826 

### Forecasted Date & Time
date = "2013-07-04" 
times = ('04:00:00', '06:00:00', '08:00:00', '10:00:00', '12:00:00', '14:00:00', '16:00:00', '18:00:00', '20:00:00', '22:00:00', '00:00:00')

### Data
temps = sc.textFile("BDA/input/temperature-readings.csv").map(lambda line: line.split(";"))
# (station, (date, time, temp))
temps = temps.map(lambda x: (x[0], (x[1], x[2], float(x[3]))))

stations = sc.textFile("BDA/input/stations.csv").map(lambda line: line.split(";"))
# (station, (lat, long))
stations = stations.map(lambda x: (x[0],(x[3], x[4])))

station_loc = stations.collectAsMap()
bc = sc.broadcast(station_loc)

# (station, (date, time, temp), (lat, long))
joined_rdd = temps.map(lambda x: (x[0], x[1], bc.value.get(x[0])))
joined_rdd = joined_rdd.filter(lambda x: x[1][0] < date)

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [float(lon1), float(lat1), float(lon2), float(lat2)])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km

def dateDist(day_1, day_2):
    """
    Returns date distance as number of days
    """
    day_1 = datetime.strptime(day_1, "%Y-%m-%d")
    day_2 = datetime.strptime(day_2, "%Y-%m-%d")
    dist  = day_1 - day_2
    return abs(dist.days)

def timeDist(time_1, time_2):
    """
    Returns time distance as number of hours
    """
    time_1 = datetime.strptime(time_1, "%H:%M:%S")
    time_2 = datetime.strptime(time_2, "%H:%M:%S")
    dist = time_1 - time_2
    return abs(dist.total_seconds()/3600)

def getKernel(h,dist):
    """
    Return the kernel given the distance function and spread parameter h
    """
    var = 2 * (h**2)
    dist = dist**2
    kernel = exp(-dist/var)
    return kernel

sums = []
for time in times:
   num, den = joined_rdd. \
       map(lambda x: (getKernel(h_date, dateDist(date, x[1][0])) + \
                      getKernel(h_distance, haversine(long, lat, x[2][1], x[2][0])) + \
                      getKernel(h_time, timeDist(time, x[1][1])), \
                      x[1][2])). \
       map(lambda x: (x[0]*x[1], x[0])). \
       reduce(lambda a,b: (a[0]+b[0], a[1]+b[1]))
   sums.append(num/den)

print(sums)
sums_file = open("sums.txt", "w+")
sums_file.write(str(sums))
sums_file.close()


mult = []
for time in times:
    num, den = joined_rdd. \
        map(lambda x: (getKernel(h_date, dateDist(date, x[1][0])) * \
                       getKernel(h_distance, haversine(long, lat, x[2][1], x[2][0])) * \
                       getKernel(h_time, timeDist(time, x[1][1])), \
                       x[1][2])). \
        map(lambda x: (x[0]*x[1], x[0])). \
        reduce(lambda a,b: (a[0]+b[0], a[1]+b[1]))
    mult.append(num/den)


print(mult)
mult_file = open("mult.txt", "w+")
mult_file.write(str(mult))
mult_file.close()