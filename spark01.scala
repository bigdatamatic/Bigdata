package scalaprojectpackage

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object spark01 {
  
  Logger.getRootLogger().setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  
  def main(args: Array[String]) {
    val conf =new SparkConf()
    .setAppName("HighWordCount").setMaster("local")
val sc=new SparkContext(conf)
    val readRDD=sc.textFile("/home/janardhan/Documents/TOI_news1.txt")
    val flatRDD=readRDD.flatMap(l => l.split(" "))
    val keyvalueRDD =flatRDD.map(words => (words,1))
    val reducebykeyRDD =keyvalueRDD.reduceByKey((v1,v2) => (v1+v2))
    val sortbyvalueRDD =reducebykeyRDD.sortBy(_._2,false)
    
    sortbyvalueRDD.foreach(println)
  }
}