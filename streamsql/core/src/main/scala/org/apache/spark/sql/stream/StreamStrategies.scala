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

import org.apache.spark.streaming.dstream.ConstantInputDStream

import org.apache.spark.sql.{StreamSQLContext, stream}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{BaseRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BuildRight, SparkLogicalPlan}

abstract class StreamStrategies extends QueryPlanner[StreamPlan] {
  self: StreamSQLContext#StreamPlanner =>

  object HashJoin extends Strategy {
    def apply(plan: LogicalPlan): Seq[StreamPlan] = plan match {
      case FilteredOperation(predicates, logical.Join(left, right, Inner, condition)) =>
        logger.debug(s"Considering join: ${predicates ++ condition}")
        // Find equi-join predicates that can be evaluated before the join, and thus can be used
        // as join keys. Note we can only mix in the conditions with other predicates because the
        // match above ensures that this is and Inner join.
        val (joinPredicates, otherPredicates) = (predicates ++ condition).partition {
          case Equals(l, r) if (canEvaluate(l, left) && canEvaluate(r, right)) ||
                               (canEvaluate(l, right) && canEvaluate(r, left)) => true
          case _ => false
        }

        val joinKeys = joinPredicates.map {
          case Equals(l,r) if canEvaluate(l, left) && canEvaluate(r, right) => (l, r)
          case Equals(l,r) if canEvaluate(l, right) && canEvaluate(r, left) => (r, l)
        }

        // Do not consider this strategy if there are no join keys.
        if (joinKeys.nonEmpty) {
          val leftKeys = joinKeys.map(_._1)
          val rightKeys = joinKeys.map(_._2)

          val joinOp = stream.StreamHashJoin(
            leftKeys, rightKeys, BuildRight, planLater(left), planLater(right))

          // Make sure other conditions are met if present.
          if (otherPredicates.nonEmpty) {
            stream.Filter(combineConjunctivePredicates(otherPredicates), joinOp) :: Nil
          } else {
            joinOp :: Nil
          }
        } else {
          logger.debug(s"Avoiding spark join with no join keys.")
          Nil
        }
      case _ => Nil
    }

    private def combineConjunctivePredicates(predicates: Seq[Expression]) =
      predicates.reduceLeft(And)

    /** Returns true if `expr` can be evaluated using only the output of `plan`. */
    protected def canEvaluate(expr: Expression, plan: LogicalPlan): Boolean =
      expr.references subsetOf plan.outputSet
  }

  object PartialAggregation extends Strategy {
    def apply(plan: LogicalPlan): Seq[StreamPlan] = plan match {
      case logical.Aggregate(groupingExpressions, aggregateExpressions, child) =>
        // Collect all aggregate expressions.
        val allAggregates =
          aggregateExpressions.flatMap(_ collect { case a: AggregateExpression => a})
        // Collect all aggregate expressions that can be computed partially.
        val partialAggregates =
          aggregateExpressions.flatMap(_ collect { case p: PartialAggregate => p})

        // Only do partial aggregation if supported by all aggregate expressions.
        if (allAggregates.size == partialAggregates.size) {
          // Create a map of expressions to their partial evaluations for all aggregate expressions.
          val partialEvaluations: Map[Long, SplitEvaluation] =
            partialAggregates.map(a => (a.id, a.asPartial)).toMap

          // We need to pass all grouping expressions though so the grouping can happen a second
          // time. However some of them might be unnamed so we alias them allowing them to be
          // referenced in the second aggregation.
          val namedGroupingExpressions: Map[Expression, NamedExpression] = groupingExpressions.map {
            case n: NamedExpression => (n, n)
            case other => (other, Alias(other, "PartialGroup")())
          }.toMap

          // Replace aggregations with a new expression that computes the result from the already
          // computed partial evaluations and grouping values.
          val rewrittenAggregateExpressions = aggregateExpressions.map(_.transformUp {
            case e: Expression if partialEvaluations.contains(e.id) =>
              partialEvaluations(e.id).finalEvaluation
            case e: Expression if namedGroupingExpressions.contains(e) =>
              namedGroupingExpressions(e).toAttribute
          }).asInstanceOf[Seq[NamedExpression]]

          val partialComputation =
            (namedGroupingExpressions.values ++
             partialEvaluations.values.flatMap(_.partialEvaluations)).toSeq

          // Construct two phased aggregation.
          stream.StreamAggregate(
            partial = false,
            namedGroupingExpressions.values.map(_.toAttribute).toSeq,
            rewrittenAggregateExpressions,
            stream.StreamAggregate(
              partial = true,
              groupingExpressions,
              partialComputation,
              planLater(child))(streamingContext))(streamingContext) :: Nil
        } else {
          Nil
        }
      case _ => Nil
    }
  }

  object BroadcastNestedLoopJoin extends Strategy {
    def apply(plan: LogicalPlan): Seq[StreamPlan] = plan match {
      case logical.Join(left, right, joinType, condition) =>
        stream.StreamBroadcastNestedLoopJoin(
          planLater(left), planLater(right), joinType, condition)(streamingContext) :: Nil
      case _ => Nil
    }
  }

  object CartesianProduct extends Strategy {
    def apply(plan: LogicalPlan): Seq[StreamPlan] = plan match {
      case logical.Join(left, right, _, None) =>
        stream.StreamCartesianProduct(planLater(left), planLater(right)) :: Nil
      case logical.Join(left, right, Inner, Some(condition)) =>
        stream.Filter(condition,
          stream.StreamCartesianProduct(planLater(left), planLater(right))) :: Nil
      case _ => Nil
    }
  }

  protected lazy val singleRowDStream = {
    new ConstantInputDStream(streamingContext,
      streamingContext.sparkContext. parallelize(Seq(new GenericRow(Array[Any]()): Row), 1))
  }

  def convertToCatalyst(a: Any): Any = a match {
    case s: Seq[Any] => s.map(convertToCatalyst)
    case p: Product => new GenericRow(p.productIterator.map(convertToCatalyst).toArray)
    case other => other
  }

  object TakeOrdered extends Strategy {
    def apply(plan: LogicalPlan): Seq[StreamPlan] = plan match {
      case logical.Limit(IntegerLiteral(limit), logical.Sort(order, child)) =>
        stream.TakeOrdered(limit, order, planLater(child))(streamingContext) :: Nil
      case _ => Nil
    }
  }

  // Can we automate these 'pass through' operations?
  object BasicOperators extends Strategy {
    // TODO: Set
    val numPartitions = 200
    def apply(plan: LogicalPlan): Seq[StreamPlan] = plan match {
      case logical.Distinct(child) =>
        stream.StreamAggregate(
          partial = false, child.output, child.output, planLater(child))(streamingContext) :: Nil
      case logical.Sort(sortExprs, child) =>
        // This sort is a global sort. Its requiredDistribution will be an OrderedDistribution.
        stream.Sort(sortExprs, global = true, planLater(child)):: Nil
      case logical.SortPartitions(sortExprs, child) =>
        // This sort only sorts tuples within a partition. Its requiredDistribution will be
        // an UnspecifiedDistribution.
        stream.Sort(sortExprs, global = false, planLater(child)) :: Nil
      case logical.Project(projectList, child) =>
        stream.Project(projectList, planLater(child)) :: Nil
      case logical.Filter(condition, child) =>
        stream.Filter(condition, planLater(child)) :: Nil
      case logical.Aggregate(group, agg, child) =>
        stream.StreamAggregate(partial = false, group, agg, planLater(child))(streamingContext) ::
          Nil
      case logical.Sample(fraction, withReplacement, seed, child) =>
        stream.Sample(fraction, withReplacement, seed, planLater(child)) :: Nil
      case logical.LocalRelation(output, data) =>
        val dataAsDStream = {
          new ConstantInputDStream(streamingContext,
            streamingContext.sparkContext.parallelize(data.map(r =>
              new GenericRow(r.productIterator.map(convertToCatalyst).toArray): Row)))
        }
        stream.ExistingDStream(output, dataAsDStream) :: Nil
      case logical.Limit(IntegerLiteral(limit), child) =>
        stream.Limit(limit, planLater(child))(streamingContext) :: Nil
      case Unions(unionChildren) =>
        stream.Union(unionChildren.map(planLater))(streamingContext) :: Nil
      case logical.Generate(generator, join, outer, _, child) =>
        stream.StreamGenerate(generator, join = join, outer = outer, planLater(child)) :: Nil
      case logical.NoRelation =>
        stream.ExistingDStream(Nil, singleRowDStream) :: Nil
      case logical.Repartition(expressions, child) =>
        stream.StreamExchange(HashPartitioning(expressions, numPartitions),
          planLater(child)) :: Nil
      case StreamLogicalPlan(existingPlan) => existingPlan :: Nil
      case t: BaseRelation if (t.isStream == false) =>
        sparkPlanWrapper(t) :: Nil
      case t @ SparkLogicalPlan(sparkPlan) => sparkPlanWrapper(t) :: Nil
      case _ => Nil
    }
  }
}
