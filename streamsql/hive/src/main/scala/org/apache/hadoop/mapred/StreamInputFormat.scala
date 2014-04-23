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

abstract class StreamInputFormat[K, V] extends InputFormat[K, V] {

  def getStreamSplits(job: JobConf, numSplits: Int): Array[StreamInputSplit]

  final def getSplits(job: JobConf, numSplits: Int): Array[InputSplit] =
    getStreamSplits(job, numSplits).map(_.asInstanceOf[InputSplit])

  def getRecordReader(split: InputSplit, conf: JobConf, reporter: Reporter): StreamRecordReader[K, V]
}

abstract class StreamInputSplit extends InputSplit {
  final def getLength(): Long = 0l
}

abstract class StreamRecordReader[K, V] extends RecordReader[K, V] {

  final def getPos() = 0l

  final def getProgress() = 0.0f
}
