package scalaprojectpackage

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level


object wordcount2 {
Logger.getRootLogger().setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  
  def main(args: Array[String]) {
    val conf =new SparkConf()
    .setAppName("HighWordCount").setMaster("local")
val sc=new SparkContext(conf)
    val readRDD=sc.textFile("/home/janardhan/Documents/TOI_news1.txt")
    .flatMap(l => l.split(" "))
    .map(words => (words,1))
    .reduceByKey((v1,v2) => (v1+v2))
    .sortBy(_._2,false)
    
    readRDD.foreach(println)
  }
}