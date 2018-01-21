'''
Glimpse of data set

user_id movie_id rating_given

115	265	2	
253	465	5	
305	451	3	
645	86	3	
624	257	2	
286	101    4	

'''
from pyspark import SparkConf, SparkContext
import collections

#Fundamental starting point from saprk framework to create SparkContext
#Can not create a sparkcontext without sparkconf

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")

# setMaster : making your local machine as master node
#RatingsHistogram : application name 

sc = SparkContext(conf = conf)

#Creating SparkContext using SparkConf

lines = sc.textFile("/data_address/u.data")
ratings = lines.map(lambda x: x.split()[2])

'''
ratings rdd will look like this

2
5
3
3
2
4

'''

result = ratings.countByValue()

'''
after countByValue result will hold data in this format

(3,2)
(2,2)
(4,1)
(5,1)

'''
sortedResults = collections.OrderedDict(sorted(result.items())) # OrderedDictionary sort by key
for key, value in sortedResults.items():
    print("%s %i" % (key, value))

Output:

2,2
3,2
4,1
5,1
