# Product Recommendation-System-with-MapReduce

Raw Data:

Input data are the customer's rating of product
<customerId>:<productId>/t<rating>

Source Code:

This progrm include 5 MapReduce jobs: ItemByUserMR, ItemRelationMR, NormCoOccurMR, MatrixMultMR and UnitSumMR.
The first 3 jobs (ItemByUserMR, ItemRelationMR and NormCoOccurMR) extract the item-to-item collaboration and build the co-occurrence matrix. The last 2 jobs (MatrixMultMR and UnitSumMR) implement the matrix multiplication and output the recommendation factor of each product to each customer.

Result:

Output data is a table list out the recommendation factor for customer-product pair
<customerId>:<productId>/t<recommendationFactor>

The higher the factor, the more recommended the product to that customer

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
