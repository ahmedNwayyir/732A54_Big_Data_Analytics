from pyspark import SparkContext
import numpy as np

sc = SparkContext(appName = "knn")
#data in the form (label, value)
data = [(0,3), (0, 5), (1, -8), (1, -2), (1, 0), (0, 9), (1,6),(0, 6), (1, 1), (1, 11), (1, -2), (1, 3)]

mydata = sc.parallelize(data)
#a point to classify
p = [-4]
k = 3
# Finding k nearest neighbors
nearestPoints = mydata.map(lambda x: (x[0], ((np.sqrt((x[1] - p[0])**2), 1)))).sortBy(lambda k: k[1][0], ascending=True).take(k)
kNearest = sc.parallelize(nearestPoints)
print(kNearest.collect())
#Choosing majority vote
votes = kNearest.map(lambda x: (x[0], x[1][1])).reduceByKey(lambda a,b: a+b).sortBy(lambda k:k[1], ascending=False).take(1)
print(f"Label for the point {votes[0][0]}")
