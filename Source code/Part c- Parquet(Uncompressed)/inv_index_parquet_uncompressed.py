from __future__ import division
from pyspark import SparkContext,SparkConf
import ntpath
from pyspark.sql import SQLContext,SparkSession
from operator import add
import sys

conf = SparkConf().setAppName("HW3_Part2_Parquet_Uncompressed")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)
sqlContext.setConf("spark.sql.parquet.compression.codec", "uncompressed")


#Read the popular words
pop_words  = sc.wholeTextFiles(sys.argv[2],use_unicode=False).map(lambda (filename,content) : content.split("\n")).flatMap(lambda word : word)
pop_words_list = pop_words.collect()

#Read the text files in the document collections
data = sc.wholeTextFiles(sys.argv[1],use_unicode=False)

# Removing special characters
content_spchars_removed = data.map(lambda (filename,content) :(ntpath.basename(filename),content.replace(",","").replace(".","").replace("/","").replace("\r","").replace("\n"," ")))

#Convert the content to lowercase
content_lowered = content_spchars_removed.map(lambda (filename,content) : (filename,content.lower(), len(content.split(" "))))

#Get the words from the content by splitting the content and filter to get only popular words
content_processed = content_lowered.map(lambda (filename,content,totalcount) : [((filename,word,totalcount),1) for word in content.split(" ") if word in pop_words_list and len(word) > 0])

#Find the counts of the key
content_count = content_processed.flatMap(lambda x : x).reduceByKey(lambda count1,count2:count1+count2)

#Map the words to get word and list of tuples containing filename and weight(word occurence/total number of occurrences) and reduceByKey to get Inverted index
inv_index = content_count.map(lambda posting : (posting[0][1],[(posting[0][0],float(posting[1])/posting[0][2])])).reduceByKey(add)

#create dataframe and write the file as parquet
inv_index_df=sqlContext.createDataFrame(inv_index)
inv_index_df.write.parquet(sys.argv[3])
