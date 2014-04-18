package org.apache.spark.sql.hive.stream

import java.util.Properties
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants._
import org.apache.hadoop.hive.ql.metadata.{Table => HiveTable}
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.io.{ Writable, LongWritable, ObjectWritable, Text }
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{JobConf}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.hive.conf.HiveConf

import org.apache.spark.SerializableWritable
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.mqtt.MQTTUtils
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.zeromq.ZeroMQUtils
import org.apache.spark.streaming.receivers.ReceiverSupervisorStrategy
import org.apache.spark.streaming.dstream.FileInputDStream

import org.apache.spark.sql.hive.StreamHiveContext
import kafka.serializer.{Decoder, StringDecoder}
import kafka.utils.VerifiableProperties
import kafka.consumer.ConsumerConfig
import akka.zeromq.Subscribe
import twitter4j.auth.Authorization
import akka.actor.{Props, SupervisorStrategy}


/**class StreamHiveContext (
  val streamingContext: StreamingContext,
  val hiveconf: HiveConf
)
*/
/**
 * A trait for subclasses that handle table scans.
 */
private[hive] trait StreamTableReader {

  def makeDStreamForTable(hiveTable: HiveTable): DStream[_]

}

/**
 * data warehouse directory.
 */
private[hive]
class CommonStreamTableReader(@transient _tableDesc: TableDesc, @transient sc: StreamHiveContext)
  extends StreamTableReader {

  private val _broadcastedHiveConf =
    sc.streamingContext.sparkContext.broadcast(new SerializableWritable(sc.hiveContext.hiveconf))

  def broadcastedHiveConf = _broadcastedHiveConf

  def hiveConf = _broadcastedHiveConf.value.value

  /**
   * Creates a DStream to read data from the source stream, and return DStream
   * that contains deserialized rows.
   *
   * @param hiveTable Hive metadata for the table being scanned.
   * @param deserializerClass Class of the SerDe used to deserialize Writables from stream source.
   */
  override def makeDStreamForTable(hiveTable: HiveTable): DStream[_] = {

    val deserializerClass = _tableDesc.getDeserializerClass.asInstanceOf[Class[Deserializer]]
    
    // Create local references to member variables, so that the entire `this` object won't be
    // serialized in the closure below.
    val tableDesc = _tableDesc
    val path = hiveTable.getPath
 
    val inputDStream = path.toUri.getScheme.toLowerCase match {
      case "kafka"  =>
        createKafkaInputDStream(tableDesc, path)
      case "flume" =>
        createFlumeInputDStream(tableDesc, path)
      case "socket" =>
        createSocketInputDStream(tableDesc, path)
      case "zeromq" =>
        createZeroMQInputDStream(tableDesc, path)
      case "twitter" =>
        createTwitterInputDStream(tableDesc, path)
      case "mqtt" =>
        createMqttInputDStream(tableDesc, path)
      case "hdfs" =>
        createHdfsInputDStream(tableDesc, path)
      case "file" =>
        createHdfsInputDStream(tableDesc, path)
/*      case "hftp" =>
        createHdfsInputDStream(tableDesc, path)
      case "hsftp" =>
        createHdfsInputDStream(tableDesc, path)
      case "har" =>
        createHdfsInputDStream(tableDesc, path)
      case "kfs" =>
        createHdfsInputDStream(tableDesc, path)
      case "ftp" =>
        createHdfsInputDStream(tableDesc, path)
      case "s3n" =>
        createHdfsInputDStream(tableDesc, path)
      case "s3" => 
        createHdfsInputDStream(tableDesc, path)

      case _ => 
        new Exception("unknown streaming scheme: " + path.toUri.getScheme)*/
    }
 
    val broadcastedHiveConf = _broadcastedHiveConf

    val deserializedDStream = inputDStream.mapPartitions { iter =>
      val hconf = broadcastedHiveConf.value.value
      val deserializer = deserializerClass.newInstance()
      deserializer.initialize(hconf, tableDesc.getProperties)

      // Deserialize each Writable to get the row value.
      iter.map {
        case v: Writable => deserializer.deserialize(v)
        case value => 
          sys.error(s"Unable to deserialize non-Writable: $value of ${value.getClass.getName}")
      }
    }
    deserializedDStream
  }

  private def getStorageLevel(properties: Properties): StorageLevel = {
    properties.getProperty("stream.storageLevel", "MEMORY_AND_DISK").toUpperCase match {
      case "DISK_ONLY" => StorageLevel(true, false, false)
      case "DISK_ONLY_2" => StorageLevel(true, false, false, 2)
      case "MEMORY_ONLY" => StorageLevel(false, true, true)
      case "MEMORY_ONLY_2" => StorageLevel(false, true, true, 2)
      case "MEMORY_ONLY_SER" => StorageLevel(false, true, false)
      case "MEMORY_ONLY_SER_2" => StorageLevel(false, true, false, 2)
      case "MEMORY_AND_DISK" => StorageLevel(true, true, true)
      case "MEMORY_AND_DISK_2" => StorageLevel(true, true, true, 2)
      case "MEMORY_AND_DISK_SER" => StorageLevel(true, true, false)
      case "MEMORY_AND_DISK_SER_2" => StorageLevel(true, true, false, 2)
      case _ => StorageLevel(true, true, true)
    }
  }

  def getPropertyFields(properties: Properties, property: String)
    : Array[(String, String)] = {
    val propertyValue: String = properties.getProperty(property)
    if (propertyValue == null) {
      new Array[(String, String)](0)
    } else {
      propertyValue.split(",").map { pair =>
        val param = pair.split("=", 2)
        if (param.size == 2) {
          (param(0).trim, param(1).trim)
        } else {
          new Exception("invalid parameter! " + pair)
          (param(0).trim, "")
        }
      }
    }
  }
  
  /**
   * Creates a kafkaDStream based on the broadcasted HiveConf and other job properties that will be
   * applied locally on each slave.
   */
  private def createHdfsInputDStream(
    tableDesc: TableDesc,
    path: Path
    ): DStream[Writable] = {
    val initializeJobConfFunc = StreamTableReader.initializeLocalJobConfFunc(tableDesc) _
    val properties = tableDesc.getProperties
    
    val filterName = properties.getProperty("stream.hdfs.filter")
    val newFilesOnly = properties.getProperty("stream.hdfs.newFilesOnly", "true").toBoolean
    val filter = if (filterName == null) {
      DefaultInputFilter
    } else {
      filterName.getClass.newInstance.asInstanceOf[InputFilter]
    }
    sc.streamingContext.fileStream[Writable, Writable, InputFormat[Writable, Writable]](
      path.toString, filter.pathFilter _, newFilesOnly).map(_._2)
  }
  
  /**
   * Creates a kafkaDStream based on the broadcasted HiveConf and other job properties that will be
   * applied locally on each slave.
   */
  private def createKafkaInputDStream(
    tableDesc: TableDesc,
    path: Path
    ): DStream[Writable] = {
    val initializeJobConfFunc = StreamTableReader.initializeLocalJobConfFunc(tableDesc) _
    val properties = tableDesc.getProperties
    
    val pathString = path.toString.split("://", 2)(1)
    val kafkaParams = getPropertyFields(properties, "stream.kafka.params").toMap + 
      ("zookeeper.connect" -> pathString)
    val topics = getPropertyFields(properties, "stream.kafka.topics").map(v => (v._1, v._2.toInt)).toMap

    val storageLevel = getStorageLevel(properties)
    val props = new Properties()
    kafkaParams.foreach(param => props.put(param._1, param._2))
    val decoder = Class.forName("stream.kafka.decoder").getConstructor(classOf[VerifiableProperties])
      .newInstance(new VerifiableProperties(props)).asInstanceOf[Decoder[Writable]]
    KafkaUtils.createStream[String, Writable, StringDecoder, KafkaDecoder](sc.streamingContext,
      kafkaParams,
      topics,
      storageLevel
      ).map(_._2)
    KafkaUtils.createStream[String, Writable](sc.streamingContext, kafkaParams, topics,
      new StringDecoder, decoder, storageLevel).map(_._2)
    // Only take the value (skip the key) because Hive works only with values.
  }
  
  /**
   *
   */
  private def createFlumeInputDStream(
    tableDesc: TableDesc,
    path: Path
    ): DStream[Writable] = {
    val initializeJobConfFunc = StreamTableReader.initializeLocalJobConfFunc(tableDesc) _
    val properties = tableDesc.getProperties

    val hostname = path.toUri.getHost 
    val port =  path.toUri.getPort 

    val storageLevel = getStorageLevel(properties)
    FlumeUtils.createStream(sc.streamingContext, hostname, port, storageLevel).map { event => 
      new ObjectWritable(event.getClass, event)
    }
  }

  private def createTwitterInputDStream(
    tableDesc: TableDesc,
    path: Path
    ): DStream[Writable] = {
    val initializeJobConfFunc = StreamTableReader.initializeLocalJobConfFunc(tableDesc) _
    val properties = tableDesc.getProperties

    val hostname = path.toUri.getHost 
    val port =  path.toUri.getPort 

    val storageLevel = getStorageLevel(properties)
    val filters = properties.getProperty("stream.twitter.filters", "").split(",").toSeq
    val authName = properties.getProperty("stream.twitter.authorization")
    val authorization = if (authName == null) {
      None
    } else {
      Some(authName.getClass.newInstance.asInstanceOf[Authorization])
    }
    TwitterUtils.createStream(sc.streamingContext, authorization, 
                              filters, storageLevel).map { status => 
      new ObjectWritable(status.getClass, status)
    }
  }
  
  
  private def createSocketInputDStream(
    tableDesc: TableDesc,
    path: Path
    ): DStream[Writable] = {
    val initializeJobConfFunc = StreamTableReader.initializeLocalJobConfFunc(tableDesc) _
    val properties = tableDesc.getProperties
    
    val hostname = path.toUri.getHost 
    val port =  path.toUri.getPort 

    val storageLevel = getStorageLevel(properties)
    val decoder = properties.getProperty("stream.decoder", DefaultStreamDecoder.getClass.getName)
    sc.streamingContext.socketStream[Writable](hostname, port, 
                              (decoder.getClass.newInstance.asInstanceOf[SocketDecoder]).streamToIterator _, 
                              storageLevel)
  }
  
  private def createZeroMQInputDStream(
    tableDesc: TableDesc,
    path: Path
    ): DStream[Writable] = {
    val initializeJobConfFunc = StreamTableReader.initializeLocalJobConfFunc(tableDesc) _
    val properties = tableDesc.getProperties

    val topic = properties.getProperty("stream.zeromq.topic","")
    val storageLevel = getStorageLevel(properties)
    val stragegyName = properties.getProperty("stream.zeromq.supervisorStrategy")
    val stragegy = if (stragegyName == null) {
      ReceiverSupervisorStrategy.defaultStrategy
    } else {
      stragegyName.getClass.newInstance.asInstanceOf[SupervisorStrategy]
    }
    val decoder = properties.getProperty("stream.decoder", DefaultStreamDecoder.getClass.getName)
    ZeroMQUtils.createStream[Writable](sc.streamingContext, path.toString, Subscribe(topic),
                                       (decoder.getClass.newInstance.asInstanceOf[ZeroMqDecoder]).bytesToObject _,
                                        storageLevel, stragegy)
  }
  
  private def createMqttInputDStream(
    tableDesc: TableDesc,
    path: Path
    ): DStream[Writable] = {
    val initializeJobConfFunc = StreamTableReader.initializeLocalJobConfFunc(tableDesc) _
    val properties = tableDesc.getProperties

    val storageLevel = getStorageLevel(properties)
    val topic = properties.getProperty("stream.mqtt.topic","")
    MQTTUtils.createStream(sc.streamingContext, path.toString, topic, storageLevel)
    .map(v => new Text(v))
  }
}

private[hive] object StreamTableReader {

  def initializeLocalJobConfFunc(tableDesc: TableDesc)(jobConf: JobConf) {
    if (tableDesc != null) {
      Utilities.copyTableJobPropertiesToConf(tableDesc, jobConf)
    }
  }
}
