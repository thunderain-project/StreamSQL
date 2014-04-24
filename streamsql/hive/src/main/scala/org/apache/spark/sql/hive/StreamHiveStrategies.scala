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

import org.apache.spark.sql.StreamSQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.stream.StreamPlan
import org.apache.spark.sql.hive.stream._

trait StreamHiveStrategies {
  self: StreamSQLContext#StreamPlanner =>

  val streamHiveContext: StreamHiveContext

  object StreamDDL extends Strategy {
    def apply(plan: LogicalPlan): Seq[StreamPlan] = plan match {
      case CreateStream(desc) => CreateStreamOperator(desc)(streamHiveContext) :: Nil
      case DropStream(desc) => DropStreamOperator(desc)(streamHiveContext) :: Nil
      case _ => Nil
    }
  }

  object StreamHiveScans extends Strategy {
    def apply(plan: LogicalPlan): Seq[StreamPlan] = plan match {
      case PhysicalOperation(projectList, predicates, relation: MetastoreRelation) =>
        filterProject(
          projectList,
          predicates,
          StreamHiveScan(_, relation)(streamHiveContext)) :: Nil
      case _ =>
        Nil
    }
  }
}
