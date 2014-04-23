package org.apache.spark.streaming.dstream

import org.apache.hadoop.mapred.{JobConf, TextSocketInputFormat}

import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SerializableWritable, SparkConf, SparkContext}
import org.apache.hadoop.io.{Text, NullWritable}

object DStreamTest {
  def main(args: Array[String]) = {
    val sc = new SparkContext("local[3]", "test", new SparkConf())
    val ssc = new StreamingContext(sc, Duration(10000))

    val jobConf = new JobConf()
    jobConf.set("streamsql.input.socket.host", "localhost")
    jobConf.set("streamsql.input.socket.port", "10080")


    val inputStream = GenericInputDStream.createStream[NullWritable, Text](
      ssc,
      jobConf,
      classOf[TextSocketInputFormat],
      1)

    inputStream.foreachRDD { r =>
      r.foreach(println)
    }

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
