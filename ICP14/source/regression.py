from pyspark.sql import *
from pyspark.sql.types import DoubleType,IntegerType
import pyspark.sql.functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import SparseVector
spark = SparkSession.builder.appName("Regression").getOrCreate()
df = spark.read.format("csv").option("header", True)\
.option("inferSchema", True).option("delimiter", ",")\
.load("imports-85.data")
data = df.withColumnRenamed("wheel-base", "label").select("label", "length", "width", "height")
data.show()
from pyspark.ml.regression import LinearRegression
assembler = VectorAssembler(inputCols=data.columns[1:], outputCol="features")
y = assembler.transform(data)
lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
model = lr.fit(y)
# Print the coefficients and intercept for linear regression
print("Coefficients: %s" % str(model.coefficients))
print("Intercept: %s" % str(model.intercept))
# Summarize the model over the training set and print out some metrics
trainingSummary = model.summary
print("numIterations: %d" % trainingSummary.totalIterations)
print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
trainingSummary.residuals.show()
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)
from pyspark.sql.functions import col, when
logistic_df = df.withColumn("label", when(col("num-of-doors") == "four", 1).otherwise(0)).select("label", "length", "width", "height")
from pyspark.ml.classification import LogisticRegression
assembler = VectorAssembler(inputCols=logistic_df.columns[1:], outputCol="features")
z = assembler.transform(logistic_df)
lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
# Fit the model
model = lr.fit(z)
# Print the coefficients and intercept for logistic regression
print("Coefficients: " + str(model.coefficients))
print("Intercept: " + str(model.intercept))
mlr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8, family="multinomial")
# Fit the model
mlr_model = mlr.fit(z)
# Print the coefficients and intercepts for logistic regression with multinomial family
print("Multinomial coefficients: " + str(mlr_model.coefficientMatrix))
print("Multinomial intercepts: " + str(mlr_model.interceptVector))