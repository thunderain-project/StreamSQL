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

import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.execution

case class StreamExchange(newPartitioning: Partitioning, child: StreamPlan) extends UnaryNode {
  def output = child.output

  lazy val sparkPlan = execution.Exchange(newPartitioning, child.sparkPlan)

  override def execute = attachTree(this, "execute") {
    child.execute().transform(_ => sparkPlan.execute())
  }
}

// TODO. Code is the same as execution.AddExchange, should refactor to remove duplicate code.
object AddExchange extends Rule[StreamPlan] {
  val numPartitions = 8

  def apply(plan: StreamPlan): StreamPlan = plan.transformUp {
    case operator: StreamPlan =>

      def meetsRequirements = !operator.requiredChildDistribution.zip(operator.children).map {
        case (required, child) =>
          val valid = child.outputPartitioning.satisfies(required)
          valid
      }.exists(!_)

      def compatible = !operator.children
        .map(_.outputPartitioning)
        .sliding(2)
        .map {
          case Seq(a) => true
          case Seq(a, b) => a compatibleWith b }
        .exists(!_)

      def addExchangeIfNecessary(partitioning: Partitioning, child: StreamPlan) =
        if (child.outputPartitioning != partitioning)
          StreamExchange(partitioning, child)
        else
           child

      if (meetsRequirements && compatible) {
        operator
      } else {
        val repartitionedChildren = operator.requiredChildDistribution.zip(operator.children).map {
          case (AllTuples, child) =>
            addExchangeIfNecessary(SinglePartition, child)
          case (ClusteredDistribution(clustering), child) =>
            addExchangeIfNecessary(HashPartitioning(clustering, numPartitions), child)
          case (OrderedDistribution(ordering), child) =>
            addExchangeIfNecessary(RangePartitioning(ordering, numPartitions), child)
          case (UnspecifiedDistribution, child) => child
          case (dist, _) => sys.error(s"Don't know how to ensure $dist")
        }
        operator.withNewChildren(repartitionedChildren)
      }
  }
}
