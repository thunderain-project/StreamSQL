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

import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties

import org.apache.hadoop.io.Text

class TextDecoder(props: VerifiableProperties = null) extends Decoder[Text] {
  def fromBytes(bytes: Array[Byte]): Text = {
    new Text(bytes)
  }
}

class TextKafkaInputFormat extends KafkaInputFormat[Text, Text, TextDecoder, TextDecoder] {

  class TextKafkaRecordReader(split: KafkaInputSplit, job: JobConf)
    extends KafkaRecordReader[Text, Text, TextDecoder, TextDecoder](split, job) {

    def setKeyAndValue(key: Text, value: Text, message: MessageAndMetadata[Text, Text]) {
      if (message.key != null) {
        key.set(message.key)
      }
      value.set(message.message)
    }
  }

  def getRecordReader(split: InputSplit, conf: JobConf, reporter: Reporter) =
    new TextKafkaRecordReader(split.asInstanceOf[KafkaInputSplit], conf)
}
