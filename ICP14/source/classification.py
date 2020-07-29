from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import DoubleType,IntegerType
import pyspark.sql.functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import SparseVector
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
spark = SparkSession.builder.appName("Classification").getOrCreate()
adult_data_df = spark.read.load("adult.csv", format="csv",delimiter=",", header=True)
adult_data_df.show()
adult_data_df.printSchema()
adult_data_df = adult_data_df.withColumn("age", adult_data_df["age"].cast(IntegerType()))
adult_data_df = adult_data_df.withColumn("fnlwgt", adult_data_df["fnlwgt"].cast(IntegerType()))
adult_data_df = adult_data_df.withColumn("educational-num", adult_data_df["educational-num"].cast(IntegerType()))
adult_data_df = adult_data_df.withColumn("capital-gain", adult_data_df["capital-gain"].cast(IntegerType()))
adult_data_df = adult_data_df.withColumn("capital-loss", adult_data_df["capital-loss"].cast(IntegerType()))
adult_data_df = adult_data_df.withColumn("hours-per-week", adult_data_df["hours-per-week"].cast(IntegerType()))
adult_data_df.printSchema()
adult_data_df.select(['hours-per-week']).show()
adult_data_df = adult_data_df.withColumn("label", adult_data_df['hours-per-week'] - 0)
adult_data_df.printSchema()
adult_data_df.select(['label']).show()
assem = VectorAssembler(inputCols=adult_data_df.columns[10:13], outputCol='features')
x = assem.transform(adult_data_df)
x.show(5)

train,test = x.randomSplit([0.6, 0.4], 1234)
nb1 = NaiveBayes(smoothing=1.0, modelType="multinomial")
model1 = nb1.fit(train)
predictions = model1.transform(test)
predictions.show(3)
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                              metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test set accuracy = " + str(accuracy))

nb2 = NaiveBayes(smoothing=10.0, modelType="multinomial")

# train the model
model2 = nb2.fit(train)

# select example rows to display.
predictions = model2.transform(test)
predictions.show(3)
# compute accuracy on the test set
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                              metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test set accuracy = " + str(accuracy))

from pyspark.ml.classification import DecisionTreeClassifier
nb3 = DecisionTreeClassifier(labelCol="label", featuresCol="features")
# train the model
model3 = nb3.fit(train)
# select example rows to display.
predictions = model3.transform(test)
predictions.show(3)
# compute accuracy on the test set
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                              metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test set accuracy = " + str(accuracy))

from pyspark.ml.classification import RandomForestClassifier
# create the trainer and set its parameters
nb3 = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=10)
# train the model
model3 = nb3.fit(train)
# select example rows to display.
predictions = model3.transform(test)
predictions.show(3)
# compute accuracy on the test set
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                              metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test set accuracy = " + str(accuracy))

from pyspark.ml.classification import RandomForestClassifier
# create the trainer and set its parameters
nb3 = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=100)
# train the model
model3 = nb3.fit(train)
# select example rows to display.
predictions = model3.transform(test)
predictions.show(3)
# compute accuracy on the test set
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                              metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test set accuracy = " + str(accuracy))