from pyspark import SparkContext,SparkConf
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

conf=SparkConf().setAppName("Duplicate_Detection")
sc=SparkContext(conf=conf)

#Read the inverted index 
inv_index_data=sc.wholeTextFiles(sys.argv[1],use_unicode=False)
inv_index=inv_index_data.flatMap(lambda (file,content):content.split("\n"))

#Map the posting to get eval(posting)- list of tuples
inverted_file=posting.map(lambda pr:pr).filter(lambda pr:len(pr)>0).map(lambda pair:(eval(pair)))

#Find the similarity matrix
sim_cal=inverted_file.map(sim_matrix).flatMap(lambda pr:pr)
similarity_matrix=sim_cal.reduceByKey(lambda c1,c2:c1+c2).sortBy(lambda x:x[1],ascending=False)
similarity_matrix.saveAsTextFile(sys.argv[2])

