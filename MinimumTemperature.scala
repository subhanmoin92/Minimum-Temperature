package com.syedabdulsubhan.spark

//importing the dependencies required
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.min

object WeatherMinimum {

 //parsing function and returning three fields : name, temperature and type
def parseline(line:String)={
  val field = line.split(",")          //using the delimiter ","
  val name = field(0)                  //first field is name
  val tp = field(2)                    //second field is temperature type MIN or MAX  
  val temp = field(3).toFloat          //third field is the temperature 
  (name, tp, temp)                     //returning the three fields
}//parseline

//main function

def main(args:Array[String]){                              //the main function starts here

  Logger.getLogger("org").setLevel(Level.ERROR)             //setting the logger for errors
                                                          
                                                            //create a sparkcontext in local
    val sc = new SparkContext("local[*]", "WeatherMinimum")

                                                            //read external source file
    val fs = sc.textFile("D:/P/Udemy/SparkScala/1800.csv")

                                                            //split the complete file
    val splitoutput = fs.map(parseline)
    
                                                              //filtering the parsed output for Minimum for second field
    val mintemp = splitoutput.filter(x => x._2 == "TMIN")
    val stationtemp = mintemp.map(x => (x._1, x._3.toFloat))        //mapping the name and temperature 
    val mintempstation = stationtemp.reduceByKey((x,y)=>min(x,y))   //reducing to minimum temperature by key
    val results = mintempstation.collect()                         //collecting the result



    for(result<-results){                                           //printing out the result in formatted way
    val station = result._1
    val temperature = result._2
    val formattedtemp = f"$temperature%.2f C"
    println(s"$station minimum temp: $formattedtemp")
    }//for


}//mmain


}//object
