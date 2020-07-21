from pyspark import SparkContext,SQLContext
import os
os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages  graphframes:graphframes:0.8.0-spark2.4-s_2.11 pyspark-shell")

from graphframes import *
from pyspark.sql import functions as f
from pyspark.sql.functions import col, lit, when, concat, desc

#---1.Import the dataset as a csv file and create data framesdirectly on import--
sc = SparkContext.getOrCreate()
sqlcontext=SQLContext(sc)

station_df= sqlcontext.read.format("csv").option("header", "true").csv('201508_station_data.csv')
trips_df= sqlcontext.read.format("csv").option("header", "true").csv('201508_trip_data.csv')
station_df.show()

#---2.Concatenate chunks into list & convert to DataFrame---
concat = station_df.select(concat("lat", lit(","),"long")).alias("location")
concat.show()

#-----3.Remove duplicates-------
station_df_dropduplicates =  station_df.dropDuplicates()
station_df_dropduplicates.show()

#-----4.Name Columns--------
station_data_df_renamecolumns=station_df.withColumnRenamed("name","Station_name").withColumnRenamed("installation","installation_date")

#------5.Output DataFrame-----
station_data_df_renamecolumns.show()

#------6.Create vertices------
vertices = station_df.withColumnRenamed("name","id").distinct()
vertices.show()

trip_edges=trips_df.withColumnRenamed("Start Station","src").withColumnRenamed("End Station","dst")


#------create graph out of the data frame created.
g = GraphFrame(vertices,trip_edges)
print(g)

#-----7.Show some vertices----
g.vertices.show()

#-----8.Show some edges-------
g.edges.show()

#-----9.Vertex in-Degree------
g.inDegrees.show()

#-----10.Vertex out-Degree-----
g.outDegrees.show()

#-----11.Apply the motif findings-----
motifs = g.find("(a)-[e]->(b); (b)-[e2]->(a)")
motifs.show()

#-----12.Stateful Queries--------
# Get Edges with duration greater than 500
g.edges.filter("duration > 764").sort("duration").show()

# Subgraphs
subgraph_vertices = g.vertices.filter("dockcount > 25")
subgraph_edges = g.edges.filter("duration > 500")
subgraph = GraphFrame(subgraph_vertices,subgraph_edges)
subgraph.vertices.show()

#Bonus
#-----1.Vertex degree---------
g.degrees.show()

#-----2.finding the common destinations-------
heighestdestination = g.edges.groupBy("src", "dst").count().orderBy(desc("count")).limit(10)
heighestdestination.show()

#----3.highest ratio of in degrees but fewest out degrees----
inDegrees = g.inDegrees
outDegrees = g.outDegrees
degreeRatio = inDegrees.join(outDegrees, inDegrees["id"] == outDegrees["id"]).drop(outDegrees["id"]).selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio")
degreeRatio.orderBy(desc("degreeRatio")).limit(10).show()

#----4.Save graphs generated to a file-------
g.vertices.coalesce(1).write.csv("vertices")
g.edges.coalesce(1).write.csv("edges")