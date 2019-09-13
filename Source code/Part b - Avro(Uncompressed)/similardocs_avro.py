from pyspark import SparkContext,SparkConf,SQLContext
import sys

conf = SparkConf().setAppName("DocSimilarity_Avro")
sc = SparkContext(conf = conf)

sqlcontext = SQLContext(sc)
sqlcontext.setConf("spark.sql.avro.compression.codec","uncompressed")
sim_matrix_df = sqlcontext.read.format("com.databricks.spark.avro").load(sys.argv[1])
sim_matrix_rdd = sim_matrix_df.rdd

similar_docs = sim_matrix_rdd.takeOrdered(10, key = lambda x: -x[1])

similar_docs = [doc[0] for doc in similar_docs]

similar_docs_1 = sc.parallelize(similar_docs)
similar_docs_1.saveAsTextFile(sys.argv[2])
