"""

Glimpse of data set


Ego Protection
Your Employer as a Security Blanket
Why it’s Worth it
Unlimited Growth Potential
Investing in Yourself, Not Someone Else
No Dependencies
No Commute


"""

import re
from pyspark import SparkConf, SparkContext



#Fundamental starting point from saprk framework to create SparkContext
#Can not create a sparkcontext without sparkconf



def changewords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")

# setMaster : making your local machine as master node
# WordCount : application name 



sc = SparkContext(conf = conf)
#Creating SparkContext using SparkConf


input = sc.textFile("/address/book.txt")
words = input.flatMap(changewords)

'''

words will be cleaned rdd after removing
all punctuations and converting all
of them into smaller words


'''


wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)


'''

map will transform each word into (word,1) tuple,
and reduce by key will give aggregated data based of keys


'''


wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()


'''

since we want too display total count first , thus 
we are swaping key and values and sorting by keys to
display in order

'''


results = wordCountsSorted.collect()

'''

Spark action to collect data

'''

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)

		
'''

print data in desired output

'''