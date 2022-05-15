package com.epam.hryshko

import org.apache.spark.sql.SparkSession
import scala.io.Source

object Consumer {
  def main(args: Array[String]) = {
    var arrHostPort: Array[String] = Array(Constants.HOST_PORT)

    val censored = Source.fromFile(Constants.CENSORED)
    var arrCensored: Array[String] = Array()
    censored.getLines().foreach{ word => arrCensored :+= word }
    censored.close()


    val spark = SparkSession
      .builder
      .appName("Spark-Kafka-Integration")
      .master("local")
//      .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.2.1")
      .getOrCreate()


//    val spark = SparkSession
//      .builder
//      .appName("Spark-Kafka-Integration")
//      .master("local")
//      .getOrCreate()

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", arrHostPort.mkString(", "))
      .option("subscribe", "json_topic")
      .option("startingOffsets", "earliest") // From starting
      .load()

    df.printSchema()



/*    val windowDuration = null

    val sc = new StreamingContext(arrHostPort.mkString(", "), "Spark-Kafka-Integration", Seconds(2))
    val sparkContext = sc.sparkContext
    sparkContext.setLogLevel("ERROR")

    val kafkaParams = Map("metadata.broker.list" -> arrHostPort.mkString(", "))
    val topics = List(Constants.TOPIC).toSet
    //val lines =

    */
  }

}

