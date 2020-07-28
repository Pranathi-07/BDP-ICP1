from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
# import requests
def aggregate_words_count(total_values, total):
    return sum(total_values) + (total or 0)
def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']
def rdd_processing(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        # Get spark sql singleton context from the current spark context
        sql_context = get_sql_context_instance(rdd.context)
        # converting the RDD to Row RDD
        rdd_Row = rdd.map(lambda w: Row(word=w[0], word_count=w[1]))
        # create a dataframe from the Row RDD created
        words_df = sql_context.createDataFrame(rdd_Row)
        words_df.registerTempTable("Words")
        # get the words from the table using SQL and print them
        words_df = sql_context.sql("select word, word_count from Words order by word_count desc")
        words_df.show()
        # words_df.saveAsTextFiles("wc_output")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)
# initializing spark context
sc = SparkContext("local[2]", "TCP Streaming word count")
# streaming context
ssc = StreamingContext(sc, 5)
ssc.checkpoint("checkpoint_TwitterApp")
# getting the data from the stream
lines = ssc.socketTextStream("localhost", 9009)
# splitting the data using space as delimiter
words = lines.flatMap(lambda line: line.split(" "))
# mapping the words as jey and value
pairs = words.map(lambda word: (word, 1))
# wordCounts = pairs.reduceByKey(lambda x, y: x + y)
# passing the words to the aggregate funtion which adds them to the previous count
words_total = pairs.updateStateByKey(aggregate_words_count)
words_total.foreachRDD(rdd_processing)
# wordCounts.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait before termination

