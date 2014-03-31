package org.apache.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ConstantInputDStream

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._

trait StrategyFactory[T <: QueryPlan[T]] {

  val project: Project[T]
  val filter: Filter[T]
  val sample: Sample[T]
  val union: Union[T]
  val stopAfter: StopAfter[T]
  val topK: TopK[T]
  val sort: Sort[T]
  val existingData: ExistingData[T]
  val equiInnerJoin: EquiInnerJoin[T]
  val cartesianProduct: CartesianProduct[T]
  val broadcastNestedLoopJoin: BroadcastNestedLoopJoin[T]
  val aggregate: Aggregate[T]
  val generate: Generate[T]
  val exchange: Exchange[T]

  trait Project[T] { def apply(projList: Seq[NamedExpression], child: T): T }

  trait Filter[T] { def apply(condition: Expression, child: T): T }

  trait Sample[T] {
    def apply(fraction: Double, withReplacement: Boolean, seed: Int, child: T): T
  }

  trait Union[T] { def apply(children: Seq[T]): T }

  trait StopAfter[T] { def apply(limit: Int, child: T): T }

  trait TopK[T] { def apply(limit: Int, sortOrder: Seq[SortOrder], child: T): T }

  trait Sort[T] { def apply(sortOrder: Seq[SortOrder], global: Boolean, child: T): T }

  trait ExistingData[T] { def apply(output: Seq[Attribute], rdd: RDD[Row]): T }

  trait EquiInnerJoin[T] {
    def apply(leftKeys: Seq[Expression], rightKeys: Seq[Expression], left: T, right: T): T
  }

  trait CartesianProduct[T] { def apply(left: T, right: T): T }

  trait BroadcastNestedLoopJoin[T] {
    def apply(streamed: T, broadcast: T, joinType: JoinType, condition: Option[Expression]): T
  }

  trait Aggregate[T] {
    def apply(partial: Boolean, groupingExprs: Seq[Expression], aggregateExprs:
      Seq[NamedExpression], child: T): T
  }

  trait Generate[T] {
    def apply(generator: Generator, join: Boolean, outer: Boolean, child: T): T
  }

  trait Exchange[T] { def apply(newPartitioning: Partitioning, child: T): T }
}

class SparkStrategyFactory(sc: SparkContext)
  extends StrategyFactory[execution.SparkPlan] {

  val project = new SparkProject
  val filter = new SparkFilter
  val sample = new SparkSample
  val union = new SparkUnion
  val stopAfter = new SparkStopAfter
  val topK = new SparkTopK
  val sort = new SparkSort
  val existingData = new SparkExistingData
  val equiInnerJoin = new SparkEquiInnerJoin
  val cartesianProduct = new SparkCartesianProduct
  val broadcastNestedLoopJoin = new SparkBroadcastNestedLoopJoin
  val aggregate = new SparkAggregate
  val generate = new SparkGenerate
  val exchange = new SparkExchange


  class SparkProject extends Project[execution.SparkPlan] {
    def apply(projList: Seq[NamedExpression], child: execution.SparkPlan) =
      execution.Project(projList, child)
  }

  class SparkFilter extends Filter[execution.SparkPlan] {
    def apply(condition: Expression, child: execution.SparkPlan) =
      execution.Filter(condition, child)
  }

  class SparkSample extends Sample[execution.SparkPlan] {
    def apply(fraction: Double, withReplacement: Boolean, seed: Int, child: execution.SparkPlan) =
      execution.Sample(fraction, withReplacement, seed, child)
  }

  class SparkUnion extends Union[execution.SparkPlan] {
    def apply(children: Seq[execution.SparkPlan]): execution.SparkPlan =
      execution.Union(children)(sc)
  }

  class SparkStopAfter extends StopAfter[execution.SparkPlan] {
    def apply(limit: Int, child: execution.SparkPlan) = execution.StopAfter(limit, child)(sc)
  }

  class SparkTopK extends TopK[execution.SparkPlan] {
    def apply(limit: Int, sortOrder: Seq[SortOrder], child: execution.SparkPlan) =
      execution.TopK(limit, sortOrder, child)(sc)
    }

  class SparkSort extends Sort[execution.SparkPlan] {
    def apply(sortOrder: Seq[SortOrder], global: Boolean, child: execution.SparkPlan) =
      execution.Sort(sortOrder, global, child)
    }

  class SparkExistingData extends ExistingData[execution.SparkPlan] {
    def apply(output: Seq[Attribute], rdd: RDD[Row]) = execution.ExistingRdd(output, rdd)
  }

  class SparkEquiInnerJoin extends EquiInnerJoin[execution.SparkPlan] {
    def apply(leftKeys: Seq[Expression], rightKeys: Seq[Expression], left: execution.SparkPlan,
        right: execution.SparkPlan) =
      execution.SparkEquiInnerJoin(leftKeys, rightKeys, left, right)
  }

  class SparkCartesianProduct extends CartesianProduct[execution.SparkPlan] {
    def apply(left: execution.SparkPlan, right: execution.SparkPlan) =
      execution.CartesianProduct(left, right)
  }

  class SparkBroadcastNestedLoopJoin extends BroadcastNestedLoopJoin[execution.SparkPlan] {
    def apply(streamed: execution.SparkPlan, broadcast: execution.SparkPlan, joinType: JoinType,
        condition: Option[Expression]) =
      execution.BroadcastNestedLoopJoin(streamed, broadcast, joinType, condition)(sc)
  }

  class SparkAggregate extends Aggregate[execution.SparkPlan] {
    def apply(partial: Boolean, groupingExprs: Seq[Expression], aggregateExprs:
        Seq[NamedExpression], child: execution.SparkPlan) =
      execution.Aggregate(partial, groupingExprs, aggregateExprs, child)(sc)
  }

  class SparkGenerate extends Generate[execution.SparkPlan] {
    def apply(generator: Generator, join: Boolean, outer: Boolean, child: execution.SparkPlan) =
      execution.Generate(generator, join, outer, child)
  }

  class SparkExchange extends Exchange[execution.SparkPlan] {
    def apply(newPartitioning: Partitioning, child: execution.SparkPlan) =
      execution.Exchange(newPartitioning, child)
  }
}

class StreamStrategyFactory(ssc: StreamingContext)
  extends StrategyFactory[stream.StreamPlan] {

  val project = new StreamProject
  val filter = new StreamFilter
  val sample = new StreamSample
  val union = new StreamUnion
  val stopAfter = new StreamStopAfter
  val topK = new StreamTopK
  val sort = new StreamSort
  val existingData = new StreamExistingData
  val equiInnerJoin = new StreamEquiInnerJoin
  val cartesianProduct = new StreamCartesianProduct
  val broadcastNestedLoopJoin = new StreamBroadcastNestedLoopJoin
  val aggregate = new StreamAggregate
  val generate = new StreamGenerate
  val exchange = new StreamExchange


  class StreamProject extends Project[stream.StreamPlan] {
    def apply(projList: Seq[NamedExpression], child: stream.StreamPlan) =
      stream.Project(projList, child)
  }

  class StreamFilter extends Filter[stream.StreamPlan] {
    def apply(condition: Expression, child: stream.StreamPlan) = stream.Filter(condition, child)
  }

  class StreamSample extends Sample[stream.StreamPlan] {
    def apply(fraction: Double, withReplacement: Boolean, seed: Int, child: stream.StreamPlan) =
      stream.Sample(fraction, withReplacement, seed, child)
  }

  class StreamUnion extends Union[stream.StreamPlan] {
    def apply(children: Seq[stream.StreamPlan]): stream.StreamPlan =
      stream.Union(children)(ssc)
  }

  class StreamStopAfter extends StopAfter[stream.StreamPlan] {
    def apply(limit: Int, child: stream.StreamPlan) = stream.StopAfter(limit, child)(ssc)
  }

  class StreamTopK extends TopK[stream.StreamPlan] {
    def apply(limit: Int, sortOrder: Seq[SortOrder], child: stream.StreamPlan) =
      stream.TopK(limit, sortOrder, child)(ssc)
    }

  class StreamSort extends Sort[stream.StreamPlan] {
    def apply(sortOrder: Seq[SortOrder], global: Boolean, child: stream.StreamPlan) =
      stream.Sort(sortOrder, global, child)
    }

  class StreamExistingData extends ExistingData[stream.StreamPlan] {
    def apply(output: Seq[Attribute], rdd: RDD[Row]) = {
      val dstream = new ConstantInputDStream[Row](ssc, rdd)
      stream.ExistingDStream(output, dstream)
    }
  }

  class StreamEquiInnerJoin extends EquiInnerJoin[stream.StreamPlan] {
    def apply(leftKeys: Seq[Expression], rightKeys: Seq[Expression], left: stream.StreamPlan,
        right: stream.StreamPlan) =
      stream.StreamEqualInnerJoin(leftKeys, rightKeys, left, right)
  }

  class StreamCartesianProduct extends CartesianProduct[stream.StreamPlan] {
    def apply(left: stream.StreamPlan, right: stream.StreamPlan) =
      stream.StreamCartesianProduct(left, right)
  }

  class StreamBroadcastNestedLoopJoin extends BroadcastNestedLoopJoin[stream.StreamPlan] {
    def apply(streamed: stream.StreamPlan, broadcast: stream.StreamPlan, joinType: JoinType,
        condition: Option[Expression]) =
      stream.StreamBroadcastNestedLoopJoin(streamed, broadcast, joinType, condition)(ssc)
  }

  class StreamAggregate extends Aggregate[stream.StreamPlan] {
    def apply(partial: Boolean, groupingExprs: Seq[Expression], aggregateExprs:
        Seq[NamedExpression], child: stream.StreamPlan) =
      stream.StreamAggregate(partial, groupingExprs, aggregateExprs, child)(ssc)
  }

  class StreamGenerate extends Generate[stream.StreamPlan] {
    def apply(generator: Generator, join: Boolean, outer: Boolean, child: stream.StreamPlan) =
      stream.StreamGenerate(generator, join, outer, child)
  }

  class StreamExchange extends Exchange[stream.StreamPlan] {
    def apply(newPartitioning: Partitioning, child: stream.StreamPlan) =
      stream.StreamExchange(newPartitioning, child)
  }
}
