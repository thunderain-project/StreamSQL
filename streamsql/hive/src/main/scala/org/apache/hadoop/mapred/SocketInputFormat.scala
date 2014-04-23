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

import java.io.{DataInput, DataOutput, DataInputStream, InputStream, IOException}
import java.net.Socket

import scala.reflect.ClassTag

import org.apache.hadoop.io.{NullWritable, Writable, Text}

case class SocketInputSplit(var host: String, var port: Int, var idx: Int)
  extends StreamInputSplit {

  require(host != null)
  require(port >= 0 && port <= 65535)

  def getLocations(): Array[String] = Array()

  def write(output: DataOutput) {
    val hostText = new Text(host)
    hostText.write(output)
    output.writeInt(port)
    output.writeInt(idx)
  }

  def readFields(input: DataInput) {
    val hostText = new Text()
    hostText.readFields(input)
    host = hostText.toString()

    port = input.readInt()
    idx = input.readInt()
  }
}

class SocketRecordReader[T <: Writable : ClassTag](split: InputSplit, conf: JobConf)
  extends StreamRecordReader[NullWritable, T] {
  def createKey(): NullWritable = NullWritable.get
  def createValue(): T = implicitly[ClassTag[T]].runtimeClass.newInstance.asInstanceOf[T]

  protected var isSocketOpened = false
  protected var inputStream: InputStream = _

  private lazy val dataInput: DataInput = {
    assert(isSocketOpened == true)
    new DataInputStream(inputStream)
  }

  protected def initInputStream(sockInputSplit: SocketInputSplit) = synchronized {
    if (!isSocketOpened) {
      val socket = new Socket(sockInputSplit.host, sockInputSplit.port)
      isSocketOpened = true
      inputStream = socket.getInputStream
    }
  }

  def close() = synchronized {
    if (isSocketOpened) {
      inputStream.close()
      inputStream = null
      isSocketOpened = false
    }
  }

  def next(key: NullWritable, value: T): Boolean = try {
    if (!isSocketOpened) {
      initInputStream(split.asInstanceOf[SocketInputSplit])
    }

    value.readFields(dataInput)
    true
  } catch {
    case e: Exception => throw new IOException(e)
  }
}

class SocketInputFormat[T <: Writable : ClassTag]
  extends StreamInputFormat[NullWritable, T] {
  def getStreamSplits(job: JobConf, numSplits: Int): Array[StreamInputSplit] =
    Array.tabulate(numSplits) { idx =>
      SocketInputSplit(job.get(SocketInputFormat.SOCKET_INPUT_HOST),
        job.get(SocketInputFormat.SOCKET_INPUT_PORT).toInt, idx)
    }

  def getRecordReader(split: InputSplit, conf: JobConf, reporter: Reporter) =
    new SocketRecordReader[T](split, conf)
}


object SocketInputFormat {
  val SOCKET_INPUT_HOST = "streamsql.input.socket.host"
  val SOCKET_INPUT_PORT = "streamsql.input.socket.port"
}