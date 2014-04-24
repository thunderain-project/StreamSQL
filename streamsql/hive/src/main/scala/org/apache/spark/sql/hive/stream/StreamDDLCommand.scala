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

package org.apache.spark.sql.hive.stream

import org.apache.hadoop.hive.ql.plan.{CreateTableDesc, DDLDesc, DropTableDesc}

import org.apache.spark.streaming.dstream.ConstantInputDStream

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.logical.Command
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.execution.ExistingRdd
import org.apache.spark.sql.hive.StreamHiveContext
import org.apache.spark.sql.stream.LeafNode

object CreateStreamDesc {
  val STREAM_CONSTANTS = ("STREAM", "TRUE")
  val STREAM_LOCATION = "STREAM_LOCATION"
}

class CreateStreamDesc extends CreateTableDesc

class DropStreamDesc extends DropTableDesc

abstract class StreamDDLCommand(ddlDesc: DDLDesc) extends Command {
  self: Product =>
}

case class DropStream(dropStreamDesc: DropStreamDesc) extends StreamDDLCommand(dropStreamDesc)

case class CreateStream(createStreamDesc: CreateStreamDesc)
  extends StreamDDLCommand(createStreamDesc)

abstract class StreamDDLOperator(ddlDesc: DDLDesc)
    (@transient streamHiveContext: StreamHiveContext)
  extends LeafNode {
  self: Product =>

  final def output = Nil
  final val sparkPlan = dummyPlan

  protected lazy val emptyRdd = streamHiveContext.streamingContext.sparkContext
    .parallelize(Seq(new GenericRow(Array[Any]()): Row), 1)
  protected lazy val dummyPlan = ExistingRdd(Nil, emptyRdd)

  def emptyResult =
    new ConstantInputDStream(streamHiveContext.streamingContext, emptyRdd)
}

case class CreateStreamOperator(@transient createStreamDesc: CreateStreamDesc)
    (@transient streamHiveContext: StreamHiveContext)
  extends StreamDDLOperator(createStreamDesc)(streamHiveContext) {
  def execute() = {
    streamHiveContext.catalog.createStream(createStreamDesc)
    emptyResult
  }
}

case class DropStreamOperator(@transient dropStreamDesc: DropStreamDesc)
    (@transient streamHiveContext: StreamHiveContext)
  extends StreamDDLOperator(dropStreamDesc)(streamHiveContext) {
  def execute() = {
    streamHiveContext.catalog.dropStream(dropStreamDesc)
    emptyResult
  }
}
