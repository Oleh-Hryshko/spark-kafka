package com.epam.hryshko


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DataTypes, StructType}

import scala.io.Source

object StreamsProcessor {
  var brokers = ""
  var kafkaTopic = ""
  var pathToTheCensored = ""
  var windowDuration = 0

  def main(args: Array[String]): Unit = {
/*
    -- [kafkaBrokerHost1:kafkaBrokerPort1,kafkaBrokerHost2:kafkaBrokerPort2] (массив хост:порт координат до кафка кластеру)
    -- kafkaTopic (назва топіка в який писати згенеровані повідомлення)
    -- /path/to/the/censored.txt (шлях до файла-словника цензури)
    -- windowDuration (розмір вікна для агрегацій вказаний у хвилинах)
*/
    if (args.length > 0) {brokers = args(0)} else {brokers = Constants.HOST_PORT}
    if (args.length > 1) {kafkaTopic = args(1)} else {kafkaTopic = Constants.TOPIC}
    if (args.length > 2) {pathToTheCensored = args(2)} else {pathToTheCensored = Constants.CENSORED}
    if (args.length > 3) {windowDuration = args(3).toInt} else {windowDuration = Constants.WINDOW_DURATION}

    new StreamsProcessor(brokers, kafkaTopic, pathToTheCensored, windowDuration).process()
  }
}

class StreamsProcessor(brokers: String, kafkaTopic: String, pathToTheCensored: String, windowMin: Int) {

  def process(): Unit = {

    System.setProperty("hadoop.home.dir", "c:\\winutil")
//    val OS: String = System.getProperty("os.name").toLowerCase
//
//    if (OS.contains("win")) System.setProperty("hadoop.home.dir", Paths.get("winutil").toAbsolutePath.toString)
//    else System.setProperty("hadoop.home.dir", "/")

    val censored = Source.fromFile(pathToTheCensored)
    var arrCensored: Array[String] = Array()
    censored.getLines().foreach{ word => arrCensored :+= word }
    censored.close()

    val spark = SparkSession.builder()
      .appName("kafka")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val inputDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "true")
      .load()

    val wordJsonDf = inputDf.selectExpr("CAST(value AS STRING)")

    val struct = new StructType()
      .add("speaker", DataTypes.StringType)
      .add("word", DataTypes.StringType)
      .add("time", DataTypes.TimestampType)

    val messageNestedDf = wordJsonDf.select(from_json($"value", struct).as("message"))

    def blackListFilter = udf((value: String) => arrCensored.map(value.contains(_)).toSeq.contains(true))

    val messageCensoredDf = messageNestedDf.selectExpr("message.*")
      .filter(blackListFilter($"word"))

    val messageDf = messageNestedDf.selectExpr("message.*")
      .filter(blackListFilter($"word")=!=true)

//    messageNestedDf.printSchema()
//    messageDf.printSchema()

    val delayThresholdMin = windowMin
    val windowDurationMin = windowMin

    import scala.concurrent.duration._

    val delayThreshold = delayThresholdMin.minutes
    val eventTime = "time"

    val valuesWatermarked = messageDf.withWatermark(eventTime, delayThreshold.toString)

    valuesWatermarked.explain

    val windowDuration = windowDurationMin.minutes

    import org.apache.spark.sql.functions.window

    val outputWindow = valuesWatermarked
      .groupBy(col("speaker"), window(col(eventTime), windowDuration.toString) as "window")
      .agg(collect_list("word") as "words")
      .select("window", "words", "speaker")

    outputWindow.printSchema()

    val consoleOutputWindow = outputWindow.writeStream
      .outputMode(OutputMode.Update)
      .format("console")
      .start()

    val outputCensored = messageCensoredDf
      .groupBy(col("speaker")).count() as "censored_count"

    outputCensored.printSchema()

    val consoleOutputCensored = outputCensored.writeStream
      .outputMode(OutputMode.Update)
      .format("console")
      .start()

    spark.streams.awaitAnyTermination()
  }

}
