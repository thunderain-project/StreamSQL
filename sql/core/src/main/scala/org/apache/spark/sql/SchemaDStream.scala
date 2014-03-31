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

package org.apache.spark.sql

import org.apache.spark.streaming.Time
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.types._

class SchemaDStream(
    @transient val streamSqlContext: StreamSQLContext,
    @transient val logicalPlan: LogicalPlan)
  extends DStream[Row](streamSqlContext.streamingContext) {

  @transient
  protected[spark] lazy val queryExecution = streamSqlContext.executePlan(logicalPlan)

  override def dependencies = List(queryExecution.toDStream)

  override def slideDuration = queryExecution.toDStream.slideDuration

  override def compute(validTime: Time): Option[RDD[Row]] = {
    queryExecution.toDStream.getOrCompute(validTime).map(_.map(_.copy()))
  }


  def select(exprs: NamedExpression*): SchemaDStream =
    new SchemaDStream(streamSqlContext, Project(exprs, logicalPlan))

  def where(condition: Expression): SchemaDStream =
    new SchemaDStream(streamSqlContext, Filter(condition, logicalPlan))

  def join(
      otherPlan: SchemaDStream,
      joinType: JoinType = Inner,
      condition: Option[Expression] = None): SchemaDStream =
    new SchemaDStream(streamSqlContext,
      Join(logicalPlan, otherPlan.logicalPlan, joinType, condition))

  def tableJoin(
      otherPlan: SchemaRDD,
      joinType: JoinType = Inner,
      condition: Option[Expression] = None): SchemaDStream = {
    new SchemaDStream(streamSqlContext,
      Join(logicalPlan, otherPlan.logicalPlan, joinType, condition))
  }

  def orderBy(sortExprs: SortOrder*): SchemaDStream =
    new SchemaDStream(streamSqlContext, Sort(sortExprs, logicalPlan))

  def groupBy(groupingExprs: Expression*)(aggregateExprs: Expression*): SchemaDStream = {
    val aliasExprs = aggregateExprs.map {
      case ne: NamedExpression => ne
      case e => Alias(e, e.toString)()
    }
    new SchemaDStream(streamSqlContext, Aggregate(groupingExprs, aliasExprs, logicalPlan))
  }

  def subquery(alias: Symbol) =
    new SchemaDStream(streamSqlContext, Subquery(alias.name, logicalPlan))

  def unionAll(otherPlan: SchemaDStream) =
    new SchemaDStream(streamSqlContext, Union(logicalPlan, otherPlan.logicalPlan))


  def where[T1](arg1: Symbol)(udf: (T1) => Boolean) =
    new SchemaDStream(
      streamSqlContext,
      Filter(ScalaUdf(udf, BooleanType, Seq(UnresolvedAttribute(arg1.name))), logicalPlan))

  def where(dynamicUdf: (DynamicRow) => Boolean) =
    new SchemaDStream(
      streamSqlContext,
      Filter(ScalaUdf(dynamicUdf, BooleanType, Seq(WrapDynamic(logicalPlan.output))), logicalPlan))

  def sample(
      fraction: Double,
      withReplacement: Boolean = true,
      seed: Int = (math.random * 1000).toInt) =
    new SchemaDStream(streamSqlContext, Sample(fraction, withReplacement, seed, logicalPlan))

  def generate(
      generator: Generator,
      join: Boolean = false,
      outer: Boolean = false,
      alias: Option[String] = None) =
    new SchemaDStream(streamSqlContext, Generate(generator, join, outer, None, logicalPlan))

  def registerAsStream(streamName: String): Unit = {
    streamSqlContext.registerDStreamAsStream(this, streamName)
  }

  def toSchemaDStream = this

  def analyze = streamSqlContext.sqlContext.analyzer(logicalPlan)
}
