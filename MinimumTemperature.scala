package com.syedabdulsubhan.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.min

object WeatherMinimum {

 //parsing function and returning three fields : name, temperature and type
def parseline(line:String)={
  val field = line.split(",")
  val name = field(0)
  val tp = field(2)
  val temp = field(3).toFloat
  (name, tp, temp)
}//parseline

//main function

def main(args:Array[String]){

  Logger.getLogger("org").setLevel(Level.ERROR)

  //create a sparkcontext in local
    val sc = new SparkContext("local[*]", "WeatherMinimum")

  //read external source file
    val fs = sc.textFile("D:/P/Udemy/SparkScala/1800.csv")

    //split the complete file
    val splitoutput = fs.map(parseline)
    //println(splitoutput)

    val mintemp = splitoutput.filter(x => x._2 == "TMIN")
    val stationtemp = mintemp.map(x => (x._1, x._3.toFloat))
    val mintempstation = stationtemp.reduceByKey((x,y)=>min(x,y))
    val results = mintempstation.collect()



    for(result<-results){
    val station = result._1
    val temperature = result._2
    val formattedtemp = f"$temperature%.2f C"
    println(s"$station minimum temp: $formattedtemp")
    }//for


}//mmain


}//object
