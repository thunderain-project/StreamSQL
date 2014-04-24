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

package org.apache.spark.sql.hive

import scala.language.implicitConversions

import java.io.File

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{ConstantInputDStream, DStream}

import org.apache.spark.sql.Row
import org.apache.spark.sql.{SchemaDStream, StreamSQLContext}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, OverrideCatalog}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, LowerCaseSchema}
import org.apache.spark.sql.catalyst.plans.logical.NativeCommand
import org.apache.spark.sql.stream.{StreamPlan, StreamPlanWrap}
import org.apache.spark.sql.hive.stream.StreamDDLCommand

class LocalStreamHiveContext(ssc: StreamingContext) extends StreamHiveContext(ssc) {

  lazy val metastorePath = new File("metastore").getCanonicalPath
  lazy val warehousePath: String = new File("warehouse").getCanonicalPath

  protected def configure() {
    hiveContext.runSqlHive(
      s"set javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=$metastorePath;create=true")
    hiveContext.runSqlHive("set hive.metastore.warehouse.dir=" + warehousePath)
  }

  configure()
}

class StreamHiveContext(ssc: StreamingContext) extends StreamSQLContext(ssc) {
  self =>

  @transient val hiveContext = new HiveContext(streamingContext.sparkContext)

  override protected[spark] def executePlan(plan: LogicalPlan): this.QueryExecution =
    new this.QueryExecution { val logical = plan }

  def streamHiveql(hqlQuery: String): SchemaDStream = {
    val result = new SchemaDStream(this, stream.StreamQl.parseSql(hqlQuery))
    result.queryExecution.toDStream
    result
  }

  def streamHql(hqlQuery: String): SchemaDStream = streamHiveql(hqlQuery)

  @transient
  protected[sql] lazy val catalog = new stream.StreamMetastoreCatalog(this)
      with OverrideCatalog {
    override def lookupRelation(
      databaseName: Option[String],
      streamName: String,
      alias: Option[String] = None): LogicalPlan = {

      LowerCaseSchema(super.lookupRelation(databaseName, streamName, alias))
    }
  }

  @transient
  protected[sql] lazy val analyzer =
    new Analyzer(catalog, HiveFunctionRegistry, caseSensitive = false)

  @transient
  val streamHivePlanner = new StreamPlanner with StreamHiveStrategies {
    val streamHiveContext = self

    override val strategies: Seq[Strategy] = Seq(
      TakeOrdered,
      PartialAggregation,
      HashJoin,
      BasicOperators,
      CartesianProduct,
      BroadcastNestedLoopJoin,
      StreamDDL,
      StreamHiveScans)

    override def sparkPlanWrapper(logicalPlan: LogicalPlan): StreamPlan = {
      val plan = hiveContext.planner(logicalPlan).next()
      val executedPlan = hiveContext.prepareForExecution(plan)
      StreamPlanWrap(executedPlan)(streamingContext)
    }
  }

  @transient
  override protected[sql] val planner = streamHivePlanner

  @transient
  protected lazy val emptyResult = new ConstantInputDStream(streamingContext,
    hiveContext.emptyResult)

  protected[sql] abstract class QueryExecution extends super.QueryExecution {
    override lazy val analyzed = analyzer(logical)

    def asRows(strs: Seq[String]) = strs.map { r =>
      new GenericRow(r.split("\t").asInstanceOf[Array[Any]]): Row
    }

    override lazy val toDStream: DStream[Row] =
      analyzed match {
        case NativeCommand(cmd) =>
          val output = hiveContext.runSqlHive(cmd)
          if (output.size == 0) {
            emptyResult
          } else {
            new ConstantInputDStream(streamingContext,
              streamingContext.sparkContext.parallelize(asRows(output), 1))
          }
        case _ =>
          executedPlan.execute().map(_.copy())
      }
  }
}
