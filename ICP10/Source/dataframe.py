import csv
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()
# --------------part 1
# -----------1.Import the dataset and create data framesdirectly on import. 
df = spark.read.csv("survey.csv",header=True)
df.printSchema()
df.createOrReplaceTempView("Survey")
df.show(5)
# -----------2.Save data to file.
# df.write.format("csv").option("header", "false").save("spark_survey3.csv")
df.write.format("csv").option("header", "true").save("output")
#-----------3.Check for Duplicate records in the dataset.
print(df.dropDuplicates().count())

df.groupBy(df.columns)\
.count()\
.where(f.col('count') > 1)\
.select(f.sum('count'))\
.show()

#---------4.Apply Union operation on the dataset and order the output by CountryName alphabetically. 

spark.sql("select * from Survey where Gender = 'Male' or Gender = 'M' or Gender='male'").createTempView("Table_Male")
spark.sql("select * from Survey where Gender = 'Female' or Gender = 'female'").createTempView("Table_Female")
spark.sql("select * from Table_Male union select * from Table_Female order by Country").show()

#----------5.Use Groupby Query based ontreatment.
spark.sql("select treatment,count(*) as count from Survey group by treatment").show()


#--------part 2 
#---------1.Apply the basic queries related to Joins and aggregate functions (at least 2)
spark.sql("select m.age,m.Country,m.Gender, m.treatment,f.Gender,f.treatment from Table_Male m join Table_Female f on m.Country = f.Country").show()
spark.sql("select sum(Age),count(Gender) from Survey").show()
spark.sql("select m.age,m.Country,m.Gender, m.treatment,f.Gender,f.treatment from Table_Male m left join Table_Female f on m.State = f.State").show()
#---------2.Write a query to fetch 13th Row in the dataset.
spark.sql("select * from Survey ORDER BY Timestamp limit 13").createTempView("Test")
spark.sql("select * from Test order by Timestamp desc limit 1").show()


