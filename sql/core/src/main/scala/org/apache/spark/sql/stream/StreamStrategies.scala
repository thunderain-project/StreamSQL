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
package stream

import org.apache.spark.streaming.StreamingContext

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.BaseRelation
import org.apache.spark.sql.execution.SparkLogicalPlan

abstract class StreamStrategies extends StrategiesFactory[StreamPlan] {
  self: StreamSQLContext#StreamPlanner =>

  val streamingContext: StreamingContext

  lazy val sparkContext = streamingContext.sparkContext
  lazy val factory = new StreamStrategyFactory(streamingContext)

  // TODO. should be changed later if stream can also use Parquet
  object StreamSpecificStrategies extends Strategy {
    def apply(plan: LogicalPlan): Seq[StreamPlan] = plan match {
      case StreamLogicalPlan(existingPlan) => existingPlan :: Nil
      case t: BaseRelation if (t.isStream == false) =>
        sparkLogicPlanToStreamPlan(t) :: Nil
      case t @ SparkLogicalPlan(sparkPlan) => sparkLogicPlanToStreamPlan(t) :: Nil
      case _ => Nil
    }
  }
}
