package com.prajwal.spark


import
org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object ExampleMain {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "ExampleMain")


    val lines = sc.textFile("hdfs://localhost:9000/user/prajwal/pg100.txt")
    val words = lines.flatMap(l => l.split("[^\\w]+"))
    val key_val = words.map(x=> (x,1))
    val word_count = key_val.reduceByKey((x,y)=>x+y)
    word_count.foreach(println)
    word_count.saveAsTextFile("abc")
    sc.stop
  }
}