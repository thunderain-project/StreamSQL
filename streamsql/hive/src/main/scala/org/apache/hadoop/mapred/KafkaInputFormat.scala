/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred

import java.io.{IOException, DataInput, DataOutput}
import java.util.Properties

import scala.reflect.ClassTag

import kafka.consumer.{Consumer, ConsumerConfig, ConsumerConnector}
import kafka.message.MessageAndMetadata
import kafka.utils.VerifiableProperties
import kafka.serializer.Decoder

import org.apache.hadoop.io.{Writable, Text}

import org.apache.spark.sql.hive.stream.CreateStreamDesc

case class KafkaInputSplit(
    var zkQuorum: String,
    var groupId: String,
    var topic: String,
    var idx: Int)
  extends StreamInputSplit {

  require(zkQuorum != null)
  require(groupId != null)
  require(topic != null)

  def getLocations(): Array[String] = Array()

  def write(output: DataOutput) {
    val zkQuorumText = new Text(zkQuorum)
    zkQuorumText.write(output)

    val groupIdText = new Text(groupId)
    groupIdText.write(output)

    val topicText = new Text(topic)
    topicText.write(output)

    output.write(idx)
  }

  def readFields(input: DataInput) {
    val zkQuorumText = new Text()
    zkQuorumText.readFields(input)
    zkQuorum = zkQuorumText.toString()

    val groupIdText = new Text()
    groupIdText.readFields(input)
    groupId = groupIdText.toString()

    val topicText = new Text()
    topicText.readFields(input)
    topic = topicText.toString()

    idx = input.readInt()
  }
}

abstract class KafkaRecordReader[
  K <: Writable : ClassTag,
  V <: Writable : ClassTag,
  U <: Decoder[K] : ClassTag,
  T <: Decoder[V] : ClassTag](split: KafkaInputSplit, job: JobConf)
  extends StreamRecordReader[K, V] {

  protected var isInitialized = false

  @transient protected var consumerConnector: ConsumerConnector = _
  @transient protected var keyDecoder: Decoder[K] = _
  @transient protected var valueDecoder: Decoder[V] = _

  private lazy val topicMessageStreams = {
    assert(isInitialized && consumerConnector != null)
    consumerConnector.createMessageStreams(Map(split.topic -> 1), keyDecoder, valueDecoder)
  }

  def createKey(): K = implicitly[ClassTag[K]].runtimeClass.newInstance().asInstanceOf[K]
  def createValue(): V = implicitly[ClassTag[V]].runtimeClass.newInstance().asInstanceOf[V]

  protected def initKafkaStream(split: KafkaInputSplit) = synchronized {
    if (!isInitialized) {
      val properties = new Properties()
      properties.put("zookeeper.connect", split.zkQuorum)
      properties.put("zookeeper.connection.timeout.ms", "10000")
      properties.put("group.id", split.groupId)

      val consumerConfig = new ConsumerConfig(properties)
      consumerConnector = Consumer.create(consumerConfig)

      keyDecoder = implicitly[ClassTag[U]].runtimeClass
        .getConstructor(classOf[VerifiableProperties])
        .newInstance(consumerConfig.props)
        .asInstanceOf[U]

      valueDecoder = implicitly[ClassTag[T]].runtimeClass
        .getConstructor(classOf[VerifiableProperties])
        .newInstance(consumerConfig.props)
        .asInstanceOf[T]

      isInitialized = true
    }
  }

  def close() = synchronized {
    if (isInitialized) {
      consumerConnector.shutdown()
      consumerConnector = null
      isInitialized = false
    }
  }

  private lazy val kafkaStream =
    Option(topicMessageStreams.valuesIterator.next()).flatMap(_.headOption).map(_.iterator())
  def next(key: K, value: V): Boolean = try {
    if (!isInitialized) {
      initKafkaStream(split)
    }

    if (kafkaStream.isDefined && kafkaStream.get.hasNext()) {
      setKeyAndValue(key, value, kafkaStream.get.next())
      true
    } else {
      false
    }
  } catch {
    case e: Exception => throw new IOException(e)
  }

  def setKeyAndValue(key: K, value: V, message: MessageAndMetadata[K, V])
}

abstract class KafkaInputFormat[
  K <: Writable : ClassTag,
  V <: Writable : ClassTag,
  U <: Decoder[K] : ClassTag,
  T <: Decoder[V]: ClassTag] extends StreamInputFormat[K, V] {

  val kafkaScheme = "^kafka://(.+):(\\d+)/topic=(.+)/group=(.+)".r

  def getStreamSplits(job: JobConf, numSplits: Int): Array[StreamInputSplit] = {
    val kafkaScheme(host, port, topic, group) = job.get(CreateStreamDesc.STREAM_LOCATION)
    Array.tabulate(numSplits) { idx =>
      KafkaInputSplit(s"$host:$port", group, topic, idx)
    }
  }
}
