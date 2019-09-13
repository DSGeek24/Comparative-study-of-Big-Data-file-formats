from pyspark import SparkContext,SparkConf
import sys

conf = SparkConf().setAppName("Duplicate_Detection")
sc = SparkContext(conf = conf)

#Read the similarity matrix
doc_similarity=sc.wholeTextFiles(sys.argv[1],use_unicode=False)
similarity=doc_similarity.flatMap(lambda (file,docsim):docsim.split("\n"))
docs_weights=similarity.map(lambda sim:sim).filter(lambda sim:len(sim)>0).map(lambda sim:(eval(sim)))

#Get the top 10 similar documents based on similarity value
similarity=docs_weights.takeOrdered(10,key=lambda x:-x[1])
most_similar_docs=sc.parallelize(similarity)
most_similar_docs.saveAsTextFile(sys.argv[2])

