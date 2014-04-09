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

package org.apache.spark.sql.stream

import org.apache.spark.streaming.StreamingContext

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution

case class StreamHashJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    buildSide: execution.BuildSide,
    left: StreamPlan,
    right: StreamPlan)
  extends BinaryNode {

  lazy val sparkPlan = execution.HashJoin(leftKeys, rightKeys, buildSide, left.sparkPlan,
    right.sparkPlan)

  def output = left.output ++ right.output
}

case class StreamCartesianProduct(
    left: StreamPlan,
    right: StreamPlan)
  extends BinaryNode {

  lazy val sparkPlan = execution.CartesianProduct(left.sparkPlan, right.sparkPlan)

  def output = left.output ++ right.output
}

case class StreamBroadcastNestedLoopJoin(
    streamed: StreamPlan,
    broadcast: StreamPlan,
    joinType: JoinType,
    condition: Option[Expression])(@transient ssc: StreamingContext)
  extends  BinaryNode {

  def left = streamed
  def right = broadcast

  lazy val sparkPlan = execution.BroadcastNestedLoopJoin(streamed.sparkPlan, broadcast.sparkPlan,
    joinType, condition)(ssc.sparkContext)

  def output = left.output ++ right.output

  override def otherCopyArgs = ssc :: Nil
}

