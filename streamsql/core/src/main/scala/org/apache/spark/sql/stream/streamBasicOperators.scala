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

import scala.reflect.runtime.universe._

import org.apache.spark.streaming.dstream.{ConstantInputDStream, DStream}
import org.apache.spark.streaming.StreamingContext

import org.apache.spark.sql.execution
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.ScalaReflection

case class Project(projectList: Seq[NamedExpression], child: StreamPlan) extends UnaryNode {
  lazy val sparkPlan = execution.Project(projectList, child.sparkPlan)
  def output = projectList.map(_.toAttribute)
}

case class Filter(condition: Expression, child: StreamPlan) extends UnaryNode {
  lazy val sparkPlan = execution.Filter(condition, child.sparkPlan)
  def output = child.output
}

case class Sample(fraction: Double, withReplacement: Boolean, seed: Int, child: StreamPlan)
  extends UnaryNode {
  lazy val sparkPlan = execution.Sample(fraction, withReplacement, seed, child.sparkPlan)
  def output = child.output
}

case class Union(children: Seq[StreamPlan])(@transient ssc: StreamingContext)
  extends StreamPlan {
  // TODO.??? have some semantic difference, has two dependencies
  lazy val sparkPlan = execution.Union(children.map(_.sparkPlan))(ssc.sparkContext)

  def output = children.head.output
  def execute() = ssc.union(children.map(_.execute()))

  override def otherCopyArgs = ssc :: Nil
}

case class Limit(limit: Int, child: StreamPlan)(@transient ssc: StreamingContext)
  extends UnaryNode {
  lazy val sparkPlan = execution.Limit(limit, child.sparkPlan)(ssc.sparkContext)
  override def otherCopyArgs = ssc :: Nil

  def output = child.output
}

case class TakeOrdered(limit: Int, sortOrder: Seq[SortOrder], child: StreamPlan)
    (@transient ssc: StreamingContext) extends UnaryNode {
  lazy val sparkPlan = execution.TakeOrdered(limit, sortOrder, child.sparkPlan)(ssc.sparkContext)
  override def otherCopyArgs = ssc :: Nil

  def output = child.output
}

case class Sort(
    sortOrder: Seq[SortOrder],
    global: Boolean,
    child: StreamPlan)
  extends UnaryNode {

  lazy val sparkPlan = execution.Sort(sortOrder, global, child.sparkPlan)

  override def execute() = attachTree(this, "sort") {
    child.execute().transform(_ => sparkPlan.execute())
  }

  def output = child.output
}

case class ExistingDStream(output: Seq[Attribute], dstream: DStream[Row]) extends LeafNode {
  val sparkPlan = execution.ExistingRdd(output, null)
  def execute() = dstream.transform { r => sparkPlan.rdd = r; r }
}

object ExistingDStream {
  def productToRowDStream[A <: Product](data: DStream[A]): DStream[Row] = data.transform { r =>
    execution.ExistingRdd.productToRowRdd(r)
  }

  def fromProductDStream[A <: Product : TypeTag](productDStream: DStream[A]) = {
    ExistingDStream(ScalaReflection.attributesFor[A], productToRowDStream(productDStream))
  }
}

case class StreamPlanWrap(sparkPlan: execution.SparkPlan)
    (@transient ssc: StreamingContext) extends LeafNode {
  val output = sparkPlan.output
  def execute() = new ConstantInputDStream(ssc, sparkPlan.execute())
}
