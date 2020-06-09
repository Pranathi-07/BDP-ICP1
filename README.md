# BDP-ICP1
#creating new directory in hdfs
hdfs dfs -mkdir user/hadoop
#copy the files from local to hdfs
hdfs dfs -copyFromLocal home/cloudera/Desktop/shakespeare.txt user/hadoop/.
hdfs dfs -copyFromLocal home/cloudera/Desktop/Desktop/word_list.txt user/hadoop/.
#appending the files
hdfs dfs -appendToFile home/cloudera/Desktop/BDP/word_list.txt user/hadoop/shakespeare.txt
#commands to get first and last five lines
hdfs dfs -cat user/hadoop/shakespeare.txt | head -5
hdfs dfs -cat user/hadoop/shakespeare.txt | tail -5
#merging the two files into a new file
hdfs dfs -cat user/hadoop/* | hdfs dfs -put - user/hadoop/output.txt
