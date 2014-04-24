package org.apache.spark.sql.hive.stream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.metadata.{Table => HiveTable}
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{StreamInputFormat, JobConf}

import org.apache.spark.SerializableWritable
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{GenericInputDStream, DStream}

import org.apache.spark.sql.hive.StreamHiveContext

/**
 * A trait for subclasses that handle table scans.
 */
private[hive] trait StreamReader extends Serializable {

  def makeDStreamForStream(hiveTable: HiveTable): DStream[_]
}


/**
 * data warehouse directory.
 */
class GenericStreamReader(tableDesc: TableDesc,
    @transient streamHiveContext: StreamHiveContext)
  extends StreamReader {

  @transient protected val ssc = streamHiveContext.streamingContext
  @transient protected val hiveContext = streamHiveContext.hiveContext

  protected val _broadcastedHiveConf = ssc.sparkContext.broadcast(
    new SerializableWritable(hiveContext.hiveconf))

  private val numReceivers = hiveConf.getInt("streamsql.input.xceivers", 1)
  private val storageLevel = getStorageLevel(
    hiveConf.get("streamsql.input.storageLevel", "MEMORY_ONLY_SER_2"))

  def broadcastedHiveConf = _broadcastedHiveConf

  def hiveConf = _broadcastedHiveConf.value.value

  override def makeDStreamForStream(hiveTable: HiveTable): DStream[_] =
    makeDStreamForStream(hiveTable,
      tableDesc.getDeserializerClass.asInstanceOf[Class[Deserializer]])

  def makeDStreamForStream(hiveTable: HiveTable,
    deserializerClz: Class[_ <: Deserializer]): DStream[_] = {

    val inputFormatClz = tableDesc.getInputFileFormatClass
      .asInstanceOf[Class[StreamInputFormat[Writable, Writable]]]
    val initJobConfFunc = StreamReader.initializeLocalJobConfFunc(tableDesc) _

    val inputDStream = GenericInputDStream.createStream(
      ssc,
      broadcastedHiveConf.asInstanceOf[Broadcast[SerializableWritable[Configuration]]],
      inputFormatClz,
      numReceivers,
      Some(initJobConfFunc),
      storageLevel).map(_._2)

    val deserializedDStream = inputDStream.mapPartitions { iter =>
      val hconf = broadcastedHiveConf.value.value
      val deserializer = deserializerClz.newInstance()
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

  protected def getStorageLevel(storageLevel: String): StorageLevel = {
    storageLevel.toUpperCase match {
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
}

private[hive] object StreamReader {
  def initializeLocalJobConfFunc(tableDesc: TableDesc)(jobConf: JobConf) {
    if (tableDesc != null) {
      Utilities.copyTableJobPropertiesToConf(tableDesc, jobConf)
      import scala.collection.JavaConversions._
      tableDesc.getProperties.foreach { case (k, v) => jobConf.set(k, v) }
    }
  }
}
