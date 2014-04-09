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

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import org.apache.spark.sql.{Logging, Row}
import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.SparkPlan

abstract class StreamPlan extends QueryPlan[StreamPlan] with Logging {
  self: Product =>

  val sparkPlan: SparkPlan

  def outputPartitioning: Partitioning = sparkPlan.outputPartitioning

  def requiredChildDistribution: Seq[Distribution] =
    sparkPlan.requiredChildDistribution

  def execute(): DStream[Row]
}

case class StreamLogicalPlan(alreadyPlanner: StreamPlan) extends logical.LogicalPlan {
  def output = alreadyPlanner.output
  def references = Set.empty
  def children = Nil
}

trait LeafNode extends StreamPlan with trees.LeafNode[StreamPlan] {
  self: Product =>
}

trait UnaryNode extends StreamPlan with trees.UnaryNode[StreamPlan] {
  self: Product =>
  def execute() = child.execute().transform(_ => sparkPlan.execute())
}

trait BinaryNode extends StreamPlan with trees.BinaryNode[StreamPlan] {
  self: Product =>
  def execute() = left.execute().transformWith(right.execute(),
    (l: RDD[Row], r: RDD[Row]) => sparkPlan.execute())
}
