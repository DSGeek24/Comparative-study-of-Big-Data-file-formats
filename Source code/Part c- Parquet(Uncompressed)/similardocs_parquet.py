from pyspark import SparkContext,SparkConf,SQLContext
import sys

conf = SparkConf().setAppName("DocSimilarity_Parquet")
sc = SparkContext(conf = conf)

sqlcontext = SQLContext(sc)
sim_matrix_df = sqlcontext.read.parquet(sys.argv[1])
sim_matrix_rdd = sim_matrix_df.rdd

similar_docs = sim_matrix_rdd.takeOrdered(10, key = lambda x: -x[1])

similar_docs = [doc[0] for doc in similar_docs]

similar_docs_1 = sc.parallelize(similar_docs)
similar_docs_1.saveAsTextFile(sys.argv[2])

