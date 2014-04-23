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

package org.apache.spark.streaming.dstream

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred._
import org.apache.hadoop.util.ReflectionUtils

import org.apache.spark.SerializableWritable
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext

class GenericInputDStream[K <: Writable, V <: Writable](
    @transient ssc: StreamingContext,
    broadcastConf: Broadcast[SerializableWritable[Configuration]],
    inputFormatCls: Class[_ <: StreamInputFormat[K, V]],
    idx: (Int, Int),
    storageLevel: StorageLevel)
  extends NetworkInputDStream[(SerializableWritable[K], SerializableWritable[V])](ssc) {

  override def getReceiver(): NetworkReceiver[(SerializableWritable[K], SerializableWritable[V])] =
    new GenericReceiver[K, V](broadcastConf, inputFormatCls, idx, storageLevel)
}

class GenericReceiver[K <: Writable, V <: Writable](
    broadcastConf: Broadcast[SerializableWritable[Configuration]],
    inputFormatCls: Class[_ <: StreamInputFormat[K, V]],
    idx: (Int, Int),
    storageLevel: StorageLevel)
  extends NetworkReceiver[(SerializableWritable[K], SerializableWritable[V])] {

  protected var isInitialized = false

  @transient protected var blockGenerator: BlockGenerator = _
  @transient protected var recordReader: StreamRecordReader[K, V] = _
  @transient protected var inputFormat = GenericInputDStream.getStreamInputFormat(inputFormatCls,
    GenericInputDStream.getJobConf(broadcastConf))

  override def getLocationPreference(): Option[String] = {
    val splits = inputFormat.getSplits(GenericInputDStream.getJobConf(broadcastConf), idx._2)
    splits(idx._1).getLocations.headOption
  }

  def onStart() {
    if (!isInitialized) {
      val conf = GenericInputDStream.getJobConf(broadcastConf)
      inputFormat = GenericInputDStream.getStreamInputFormat(inputFormatCls, conf)
      val split = inputFormat.getSplits(conf, idx._2)(idx._1)
      recordReader = inputFormat.getRecordReader(split, conf, Reporter.NULL)
      blockGenerator = new BlockGenerator(storageLevel)
      blockGenerator.start()
      isInitialized = true
    }

    val key: K = recordReader.createKey()
    val value: V = recordReader.createValue()

    while(recordReader.next(key, value)) {
      blockGenerator += (new SerializableWritable[K](key), new SerializableWritable[V](value))
    }
  }

  def onStop() {
    if (isInitialized) {
      recordReader.close()
      isInitialized = false
    }
    blockGenerator.stop()
  }
}

object GenericInputDStream {
  def createStream[K <: Writable, V <: Writable](
    ssc: StreamingContext,
    conf: JobConf,
    inputFormatCls: Class[_ <: StreamInputFormat[K, V]],
    numReceivers: Int,
    storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_SER_2): DStream[(K, V)] = {
    val inputStreams = Array.tabulate(numReceivers) { idx =>
      new GenericInputDStream[K, V](ssc,
        ssc.sc.broadcast(new SerializableWritable(conf)),
        inputFormatCls,
        (idx, numReceivers),
        storageLevel)
    }

    ssc.union(inputStreams).map { case (k, v) => (k.value, v.value)}
  }

  def getStreamInputFormat[K, V](
    inputFormatCls: Class[_ <: StreamInputFormat[K, V]],
    conf: JobConf): StreamInputFormat[K, V] = {
    val newInputFormat = ReflectionUtils.newInstance(inputFormatCls.asInstanceOf[Class[_]], conf)
      .asInstanceOf[StreamInputFormat[K, V]]
    if (newInputFormat.isInstanceOf[Configurable]) {
      newInputFormat.asInstanceOf[Configurable].setConf(conf)
    }
    newInputFormat
  }

  def getJobConf(broadcastConf: Broadcast[SerializableWritable[Configuration]])
    : JobConf = broadcastConf.value.value.asInstanceOf[JobConf]
}
