package org.apache.spark.sql.hive.stream

import org.apache.hadoop.hive.serde2.objectinspector._

import org.apache.spark.streaming.dstream.DStream

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.ExistingRdd
import org.apache.spark.sql.hive.{MetastoreRelation, HiveInspectors, StreamHiveContext}
import org.apache.spark.sql.stream.LeafNode

/* Implicits */
import scala.collection.JavaConversions._

case class StreamHiveScan(
    attributes: Seq[Attribute],
    relation: MetastoreRelation)(
    @transient val streamHiveContext: StreamHiveContext)
  extends LeafNode with HiveInspectors {

  lazy val sparkPlan = ExistingRdd(output, null)

  @transient
  val streamReader = new GenericStreamReader(relation.tableDesc, streamHiveContext)

  @transient
  lazy val objectInspector =
    relation.tableDesc.getDeserializer.getObjectInspector.asInstanceOf[StructObjectInspector]

  @transient
  protected lazy val attributeFunctions: Seq[(Any) => Any] = {
    attributes.map { a =>
      val ref = objectInspector.getAllStructFieldRefs
        .find(_.getFieldName == a.name)
        .getOrElse(sys.error(s"Can't find attribute $a"))
      (row: Any) => {
        val data = objectInspector.getStructFieldData(row, ref)
        unwrapData(data, ref.getFieldObjectInspector)
      }
    }
  }

  @transient
  def inputDStream: DStream[_] = streamReader.makeDStreamForStream(relation.hiveQlTable)

  def execute() = {
    inputDStream.map { row =>
      val values = attributeFunctions.map(_(row))
      new GenericRow(values.map {
        case n: String if n.toLowerCase == "null" => null
        case varchar: org.apache.hadoop.hive.common.`type`.HiveVarchar => varchar.getValue
        case decimal: org.apache.hadoop.hive.common.`type`.HiveDecimal =>
          BigDecimal(decimal.bigDecimalValue)
        case other => other
      }.toArray).asInstanceOf[Row]
    }.transform{ r => sparkPlan.rdd = r; r }
  }

  def output = attributes
}
