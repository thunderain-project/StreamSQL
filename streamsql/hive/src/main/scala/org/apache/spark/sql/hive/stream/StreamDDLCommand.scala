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

import org.apache.spark.sql.catalyst.plans.logical.Command

object CreateStreamDesc {
  val STREAM_CONSTANTS = ("STREAM", "TRUE")
}

class CreateStreamDesc extends CreateTableDesc

class DropStreamDesc extends DropTableDesc

abstract class StreamDDLCommand(ddlDesc: DDLDesc) extends Command {
  self: Product =>
}

case class DropStream(dropStreamDesc: DropStreamDesc) extends StreamDDLCommand(dropStreamDesc)

case class CreateStream(createStreamDesc: CreateStreamDesc)
  extends StreamDDLCommand(createStreamDesc)
