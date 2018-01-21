'''

Glimpse of data set

user(id) name age friends

0	Will	33	385
1	Jean	26	2
2	Hugh	55	221
3	Deanna	40	465
4	Quark	68	21
5	Weyoun	59	318

'''


from pyspark import SparkConf, SparkContext


#Fundamental starting point from saprk framework to create SparkContext
#Can not create a sparkcontext without sparkconf


conf = SparkConf().setMaster("local").setAppName("Friends")

#Creating SparkContext using SparkConf
sc = SparkContext(conf = conf)


# setMaster : making your local machine as master node
#Friends : application name 

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("/address/friends.csv")

rdd = lines.map(parseLine)

'''

rdd will be containing data like,

(33,385)
(26,2)
(55,221)
(40,465)

'''

totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

'''
#reduce by key is an action, nothing actually happens untill you call an action 
map values is coverting data like,

(33,(385,1))
(33,(200,1))

reduceByKey is coverting data like,

(33,(585,2))

'''

averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

results = averagesByAge.collect() #Collect is an action

'''

after average by values data will be like,

(33,585/2)


'''


for result in results:
    print(result)
