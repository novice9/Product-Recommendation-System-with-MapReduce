# Recommendation-System-with-MapReduce

Raw Data:


Source Code:


Result:

--------------------------------------------------
How to run Hadoop MapReduce:

 1. Upload input files into hadoop file system:
    
    $ hdfs fs -mkdir /input
    
    $ hdfs fs -put data/* /input/
    
 2. Generate jar file from your java mapReduce program
    
    $ hadoop com.sun.tools.javac.Main *.java
    
    $ jar cf recommender.jar *.class
 
 3. Run MapReduce on Hadoop
  
    $ hadoop jar recommender.jar Driver /input /output

 4. Check the result from MapReduce
 
    $ hdfs fs -cat /library/part-r-00000
