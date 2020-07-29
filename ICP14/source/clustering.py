from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import DoubleType,IntegerType
import pyspark.sql.functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
import pandas as pd
spark = SparkSession.builder.appName("KMeans").getOrCreate()
df = spark.read.csv("diabetic_data.csv", header=True, inferSchema=True)
diabetes_df = df.select("admission_type_id", "discharge_disposition_id", "admission_source_id", "time_in_hospital", "num_lab_procedures")
diabetes_df.head()
assembler = VectorAssembler(inputCols=diabetes_df.columns, outputCol="features")
data = assembler.transform(diabetes_df)
#k-means model.
kmeans = KMeans().setK(2).setSeed(1)
model = kmeans.fit(data)
# Make predictions
predictions = model.transform(data)
# Shows the result.
ctr=[]
centers = model.clusterCenters()
for center in centers:
    ctr.append(center)
    print(center)
pandasDF=predictions.toPandas()
centers = pd.DataFrame(ctr,columns=diabetes_df.columns)
print(pandasDF)