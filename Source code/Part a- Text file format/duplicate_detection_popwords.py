from pyspark import SparkContext,SparkConf,SQLContext
from operator import add
from nltk.corpus import stopwords
#from nltk.stem.porter import *
import sys
import os

conf = SparkConf().setAppName("Duplicate_Detection")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

#Read the text files 
data = sc.wholeTextFiles(sys.argv[1],use_unicode=False)
#Split the words 
words = data.flatMap(lambda l: l[1].lower().split())

#Replace special characters with empty string
words=words.map(lambda w:w.replace(",","").replace(".","").replace("/","").replace("/n","").replace("/r",""))

#Set stop words downloaded from nltk
stop_words = set(stopwords.words('english'))

#Filter the stop words 
filtered_words = words.filter(lambda w: w.lower() not in stop_words)
#stemmer=PorterStemmer()
#words_3= words_2.map(lambda w:stemmer.stem(w)) 

#Give a count of 1 for each occurrence of a certain word
word_count = filtered_words.map(lambda w:(w.lower(),1))

#Use reduceByKey to find the total count of each word
count = word_count.reduceByKey(add)

#Find the top 1000 popular words based on the value(count) of the key(word)
popular_words=count.takeOrdered(1000,key=lambda x:-x[1])

#Extract only the popular words
popular_words=[i[0] for i in popular_words]

#Convert the list of popular words to RDD
pop_words_rdd=sc.parallelize(popular_words)

#Save the popular words into a text file to be read in Part 2.
pop_words_rdd.saveAsTextFile(sys.argv[2])

