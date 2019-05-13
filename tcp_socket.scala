package scalaprojectpackage

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object tcp_socket {

def main(args: Array[String]) {
    val conf =new SparkConf()
    .setAppName("HighWordCount").setMaster("local")
val sc=new SparkContext(conf)
    
    // Create a StreamingContext with a 1-second batch size from a SparkConf
val ssc = new StreamingContext(conf, Seconds(15))
// Create a DStream using data received after connecting to port 7777 on the
val lines = ssc.socketTextStream("localhost", 7777)
// Filter our DStream for lines with "error"
val DebugLines = lines.filter(_.contains("DEBUG"))
// Print out the lines with errors
DebugLines.print()
}
}