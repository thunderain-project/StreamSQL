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

import scala.reflect.runtime.universe._

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.dsl
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.stream._

class StreamSQLContext(@transient val streamingContext: StreamingContext)
  extends Logging
  with dsl.ExpressionConversions
  with Serializable {

  self =>

  @transient val sqlContext = new SQLContext(streamingContext.sparkContext)

  protected[sql] def executeSql(sql: String): this.QueryExecution =
    executePlan(sqlContext.parseSql(sql))

  protected[spark] def executePlan(plan: LogicalPlan): this.QueryExecution =
    new this.QueryExecution { val logical = plan }

  implicit def createSchemaDStream[T <: Product : TypeTag](dstream: DStream[T]) =
    new SchemaDStream(this, StreamLogicalPlan(ExistingDStream.fromProductDStream(dstream)))

  def registerDStreamAsStream(dstream: SchemaDStream, streamName: String): Unit =
    sqlContext.catalog.registerTable(None, streamName, dstream.logicalPlan)

  def streamSql(sqlText: String): SchemaDStream = {
    val result = new SchemaDStream(this, sqlContext.parseSql(sqlText))
    result.queryExecution.toDStream
    result
  }

  protected[sql] class StreamPlanner extends StreamStrategies {
    val streamingContext = self.streamingContext

    val strategies: Seq[Strategy] =
      TakeOrdered ::
      PartialAggregation ::
      HashJoin ::
      BasicOperators ::
      CartesianProduct ::
      BroadcastNestedLoopJoin :: Nil

    def sparkPlanWrapper(logicalPlan: LogicalPlan): StreamPlan = {
      val plan = sqlContext.planner(logicalPlan).next()
      val executedPlan = sqlContext.prepareForExecution(plan)
      StreamPlanWrap(executedPlan)(streamingContext)
    }
    
    def filterProject(projectList: Seq[NamedExpression],
        filterPredicates: Seq[Expression],
        scanBuilder: Seq[Attribute] => StreamPlan): StreamPlan = {
      val projectSet = projectList.flatMap(_.references).toSet
      val filterSet = filterPredicates.flatMap(_.references).toSet
      val filterCondition = filterPredicates.reduceLeftOption(And)

      if (projectList.toSet == projectSet && filterSet.subsetOf(projectSet)) {
        val scan = scanBuilder(projectList.asInstanceOf[Seq[Attribute]])
        filterCondition.map(Filter(_, scan)).getOrElse(scan)
      } else {
        val scan = scanBuilder((projectSet ++ filterSet).toSeq)
        Project(projectList, filterCondition.map(Filter(_, scan)).getOrElse(scan))
      }
    }
  }

  @transient
  protected[sql] val planner = new StreamPlanner

  @transient
  protected[sql] val prepareForExecution = new RuleExecutor[StreamPlan] {
    val batches =
      Batch("Add exchange", Once, AddExchange) ::
      Batch("Prepare Expressions", Once, new BindReferences[StreamPlan]) :: Nil
  }

  protected abstract class QueryExecution {
    def logical: LogicalPlan

    lazy val analyzed = sqlContext.analyzer(logical)
    lazy val optimizedPlan = sqlContext.optimizer(analyzed)

    lazy val streamPlan = planner(optimizedPlan).next()
    lazy val executedPlan: StreamPlan = prepareForExecution(streamPlan)

    lazy val toDStream: DStream[Row] = executedPlan.execute()
  }
}
