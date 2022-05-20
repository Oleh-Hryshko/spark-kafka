package com.epam.hryshko

object Constants {
  val HOST_PORT = "localhost:9092"
  val TOPIC = "myTopic"
  val INPUT_DIR = "c:/Users/29a/IdeaProjects/spark-kafka/src/main/input/"
  val SPEAKERS: String = INPUT_DIR + "speakers.txt"
  val DICTIONARY: String = INPUT_DIR + "dictionary.txt"
  val CENSORED: String = INPUT_DIR + "censored.txt"
  val WINDOW_DURATION: Int = 60
}
