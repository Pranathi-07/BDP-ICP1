from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "TCP Streaming word count")
ssc = StreamingContext(sc, 5)

# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 8086)

# Split each line into words
counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda x: (len(x),x)) \
        .reduceByKey(lambda a, b: a + "," + b)

counts.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate