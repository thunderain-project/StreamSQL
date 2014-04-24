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
import org.apache.spark.streaming.receiver.Receiver

class GenericInputDStream[K <: Writable, V <: Writable](
    @transient ssc: StreamingContext,
    broadcastConf: Broadcast[SerializableWritable[Configuration]],
    initLocalJobConfFuncOpt: Option[JobConf => Unit],
    inputFormatCls: Class[_ <: StreamInputFormat[K, V]],
    idx: (Int, Int),
    storageLevel: StorageLevel)
  extends ReceiverInputDStream[(SerializableWritable[K], SerializableWritable[V])](ssc) {

  override def getReceiver(): Receiver[(SerializableWritable[K], SerializableWritable[V])] =
    new GenericReceiver[K, V](broadcastConf,
      initLocalJobConfFuncOpt,
      inputFormatCls,
      idx,
      storageLevel)
}

class GenericReceiver[K <: Writable, V <: Writable](
    broadcastConf: Broadcast[SerializableWritable[Configuration]],
    initLocalJobConfFuncOpt: Option[JobConf => Unit],
    inputFormatCls: Class[_ <: StreamInputFormat[K, V]],
    idx: (Int, Int),
    storageLevel: StorageLevel)
  extends Receiver[(SerializableWritable[K], SerializableWritable[V])](storageLevel) {

  import GenericInputDStream.getJobConf
  import GenericInputDStream.getStreamInputFormat

  protected var isInitialized = false

  @transient protected var recordReader: StreamRecordReader[K, V] = _
  @transient protected var inputFormat = getStreamInputFormat(inputFormatCls,
    getJobConf(broadcastConf, initLocalJobConfFuncOpt))

  @transient var receivingThread: Thread = null

  override def preferredLocation(): Option[String] = {
    val splits = inputFormat.getSplits(getJobConf(broadcastConf, initLocalJobConfFuncOpt), idx._2)
    splits(idx._1).getLocations.headOption
  }

  def onStart() {
    receivingThread = new Thread("Generic Receiver") {
      override def run() {
        initialize()
        receive()
      }
    }
    receivingThread.start()
 }

  def onStop() {
    if (isInitialized) {
      recordReader.close()
      isInitialized = false
    }
    if (receivingThread != null) {
      receivingThread.join()
    }
  }

  protected def initialize() {
    if (!isInitialized) {
      val conf = GenericInputDStream.getJobConf(broadcastConf, initLocalJobConfFuncOpt)
      inputFormat = GenericInputDStream.getStreamInputFormat(inputFormatCls, conf)
      val split = inputFormat.getSplits(conf, idx._2)(idx._1)
      recordReader = inputFormat.getRecordReader(split, conf, Reporter.NULL)

      isInitialized = true
    }
  }

  protected def receive() {
    val key: K = recordReader.createKey()
    val value: V = recordReader.createValue()

    while(recordReader.next(key, value)) {
      store((new SerializableWritable[K](key), new SerializableWritable[V](value)))
    }

  }
}

object GenericInputDStream {
  def createStream[K <: Writable, V <: Writable](
    ssc: StreamingContext,
    conf: JobConf,
    inputFormatCls: Class[_ <: StreamInputFormat[K, V]],
    numReceivers: Int,
    initJobConfFuncOpt: Option[JobConf => Unit] = None,
    storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_SER_2): DStream[(K, V)] = {
    createStream(ssc,
      ssc.sc.broadcast(new SerializableWritable(conf))
        .asInstanceOf[Broadcast[SerializableWritable[Configuration]]],
      inputFormatCls,
      numReceivers,
      initJobConfFuncOpt,
      storageLevel)
  }

  def createStream[K <: Writable, V <: Writable](
    ssc: StreamingContext,
    broadcastConf: Broadcast[SerializableWritable[Configuration]],
    inputFormatCls: Class[_ <: StreamInputFormat[K, V]],
    numReceivers: Int,
    initJobConfFuncOpt: Option[JobConf => Unit],
    storageLevel: StorageLevel): DStream[(K, V)] = {
    val inputStreams = Array.tabulate(numReceivers) { idx =>
      new GenericInputDStream[K, V](ssc,
        broadcastConf,
        initJobConfFuncOpt,
        inputFormatCls,
        (idx, numReceivers),
        storageLevel)
    }

    ssc.union(inputStreams).map { case (k, v) => (k.value, v.value) }
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

  def getJobConf(broadcastConf: Broadcast[SerializableWritable[Configuration]],
      initLocalJobConfFuncOpt: Option[JobConf => Unit]): JobConf = {
    val conf = broadcastConf.value.value
    if (conf.isInstanceOf[JobConf]) {
      conf.asInstanceOf[JobConf]
    } else {
      val newJobConf = new JobConf(conf)
      initLocalJobConfFuncOpt.map(f => f(newJobConf))
      newJobConf
    }
  }
}
