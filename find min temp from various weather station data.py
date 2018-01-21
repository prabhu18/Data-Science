
'''

Glimpse of data set

station(id)  timestamp  min/max/other  temp(*f)

ITE00100554	18000101	TMAX	-75
ITE00100554	18000101	TMIN	+75
GM000010962	18000101	PRCP	-45
EZE00100082	18000101	TMAX	-15
EZE00100082	18000101	TMIN	-45
ITE00100554	18000102	TMAX	-77

'''


from pyspark import SparkConf, SparkContext



#Fundamental starting point from saprk framework to create SparkContext
#Can not create a sparkcontext without sparkconf


conf = SparkConf().setMaster("local").setAppName("find min temperature")
# setMaster : making your local machine as master node
#find min temperature : application name 

#Creating SparkContext using SparkConf
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("/address/1800_weather_data.csv")
parsedLines = lines.map(parseLine)

'''
parsedLines will contain data like 

( ITE00100554,TMAX,-75)
.
.
'''


minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])

'''

Filter out all , will contain only sets with TMIN values

'''

stationTemps = minTemps.map(lambda x: (x[0], x[2]))

'''

will create new mapping and exclude Tmin/Tmax field

'''

minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))


'''

will reduce by key and keep only min temp for all keys

'''
results = minTemps.collect();

'''

spark action to collect the data

'''
for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
	
'''

printing in your desired format

'''
