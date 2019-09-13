from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext
import sys

#Function to find the similarity values for each pair of documents
def sim_matrix(pair):
    similarity_mat=[]
    pair=pair[1]
    for i in range(len(pair)):
        for j in range(i+1, len(pair)):
           if(len(pair)>1):
               wt1=pair[i][1]
               wt2=pair[j][1]
               sim=((pair[i][0],pair[j][0]),wt1*wt2)
               similarity_mat.append(sim)
    return similarity_mat

conf=SparkConf().setAppName("HW3_Part3_Parquet_Uncompressed")
sc=SparkContext(conf=conf)
sqlContext=SQLContext(sc)
sqlContext.setConf("spark.sql.parquet.compression.codec", "uncompressed")

#Read the inverted index as dataframe and convert it into rdd
inv_index_data_parquet = sqlContext.read.parquet(sys.argv[1])
inv_index_rdd=inv_index_data_parquet.rdd

#Find similarity matrix
inverted_file=inv_index_rdd.map(lambda pr:pr).filter(lambda pr:len(pr)>0).map(lambda pair:pair)
sim_cal=inverted_file.map(sim_matrix).flatMap(lambda pr:pr)
similarity_matrix=sim_cal.reduceByKey(lambda c1,c2:c1+c2).sortBy(lambda x:x[1],ascending=False)

#Create a dataframe and write as parquet file 
sim_matrix_df=sqlContext.createDataFrame(similarity_matrix)
sim_matrix_df.write.parquet(sys.argv[2])
