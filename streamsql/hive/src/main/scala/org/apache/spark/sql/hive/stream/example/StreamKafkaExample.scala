package org.apache.spark.sql.hive.stream

import java.util.{Properties, Random}
import java.io.{FileReader, BufferedReader, FileInputStream, IOException}
import kafka.producer._

import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.hive.conf.HiveConf

// scalastyle:off
/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: KafkaWordCount <master> <zkQuorum> <group> <topics> <numThreads>
 *   <master> is the Spark master URL. In local mode, <master> should be 'local[n]' with n > 1.
 *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
 *   <group> is the name of kafka consumer group
 *   <topics> is a list of one or more kafka topics to consume from
 *   <numThreads> is the number of threads the kafka consumer should use
 *
 * Example:
 *    `./bin/run-example org.apache.spark.streaming.examples.KafkaWordCount local[2] zoo01,zoo02,zoo03 my-consumer-group topic1,topic2 1`
 */
// scalastyle:on

object StreamTableReaderKafka {
  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: KafkaStreamTableReader <master> <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(master, zkQuorum, group, topics, numThreads) = args

    val ssc =  new StreamingContext(master, "KafkaStreamTableReader", Seconds(2),
      System.getenv("SPARK_HOME"), StreamingContext.jarOfClass(this.getClass))
    ssc.checkpoint("checkpoint")

 //   val streamHiveContext = new StreamHiveContext(ssc, new HiveConf()) // used for testing
 //   val topicpMap = topics.split(",").map((_,numThreads.toInt)).toMap
    
  }
}

// Produces some random words between 1 and 100.
object KafkaMessageProducer {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: KafkaMessageProducer <metadataBrokerList> <topic> " +
        "<messagesPerSec> <seedMsgFilePath>")
      System.exit(1)
    }

    val Array(brokers, topic, messagesPerSec, seedMsgFilePath) = args

    // Zookeper connection properties
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    
    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)
    val msgBuffer = new ArrayBuffer[String]
    var reader: BufferedReader = null
    
    try {
      reader = new BufferedReader(new FileReader(seedMsgFilePath))
      var str = reader.readLine()
      while (str != null) {
        msgBuffer += str
        str = reader.readLine()
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        reader.close()
        System.exit(-1)
    } finally {
      reader.close()
    }
    val msgArray = msgBuffer.toArray
    val random = new Random()

    // Send some messages
    while(true) {
      val messages = (1 to messagesPerSec.toInt).map { messageNum =>
        val msg = msgArray(random.nextInt(msgArray.size))

        new KeyedMessage[String, String](topic, msg)
      }.toArray

      producer.send(messages: _*)
      Thread.sleep(100)
    }
  }

}
