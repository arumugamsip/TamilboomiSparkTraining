package com.tamilboomi.fileoperations
import org.apache.spark.sql.SparkSession
object sparkfileformat {
  def main(args:Array[String])
  {
     val spark:SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("tamilboomi.com")
    .getOrCreate()

   /*println("##spark read text files from a directory into RDD")
   val rddFromFile = spark.sparkContext.textFile("src/main/resources/Crime_Data.csv")
   rddFromFile.foreach(f=>println(f))*/
   
   println("##Spark read text file from a directory to dataframe")
   val dataFromFileDF = spark.read.option("header",true).csv("src/main/resources/Crime_Data.csv") 
   dataFromFileDF.show(10)
   
    
  }
}