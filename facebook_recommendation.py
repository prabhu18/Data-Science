import sys
from pyspark import SparkConf, SparkContext
from math import sqrt

conf = SparkConf()
sc = SparkContext(conf = conf)



#aws s3 cp s3://prabhatsbucket/facebook_recommendation.py ./
#spark-submit --executor-memory 1g facebook_recommendation.py 340


def getthelist(line):
    fields = line.split()
    userID =int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(connection)

    return (userID, (connections))


def createStartingRdd():
    inputFile = sc.textFile("s3n://prabhatsbucket/Friend-Graph.txt")
    return inputFile.map(getthelist)


def filterDuplicates( userMappings ):
	user1 = userMappings[0][0]
	user2 = userMappings[1][0]
	return user1 != user2
	
	
def makePairs(combodata):
	user1 = combodata[0][0]
	user2 = combodata[1][0]
	
	friendlist1=combodata[0][1]
	friendlist2=combodata[1][1]
	
	common=set(friendlist1).intersection(friendlist2)
	
	key= (user1,user2) if(user1>user2) else (user2,user1)
	
	return (key,len(common))
	


iterationRdd = createStartingRdd()

MappingsPartitioned = iterationRdd.partitionBy(100)

combo = MappingsPartitioned.cartesian(MappingsPartitioned)

uniqueJoinedMappings = combo.filter(filterDuplicates)

userPairs = uniqueJoinedMappings.map(makePairs)

userPairsfinal = userPairs.reduceByKey(lambda x,y:x)



if (len(sys.argv) > 1):

    user = int(sys.argv[1])

    filteredResults = userPairsfinal.filter(lambda pair:(pair[0][0] == user or pair[0][1] == user) )
    results = filteredResults.map(lambda pairFriend: (pairFriend[1], pairFriend[0])).sortByKey(ascending = False).take(10)

    print("Top 10 recommended friend for " + str(user)) 
    for result in results:
        (count, pair) = result
        friend = pair[0]
        if (friend == user):
            friend = pair[1]
        print friend, count

