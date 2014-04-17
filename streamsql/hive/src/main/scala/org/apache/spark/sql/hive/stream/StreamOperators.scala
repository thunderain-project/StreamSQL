package org.apache.spark.sql.hive.stream

import org.apache.hadoop.hive.serde2.Serializer
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.mapred._

import org.apache.spark.sql.hive.StreamHiveContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.hive.{ MetastoreRelation, HiveInspectors }
import org.apache.spark.sql.stream._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.streaming.dstream.DStream
import scala.Some
import scala.collection.immutable.ListMap

/* Implicits */
import scala.collection.JavaConversions._

// ToDo where is StreaSqlContext Created
case class StreamHiveTableScan(
    attributes: Seq[Attribute],
    relation: MetastoreRelation)(
    @transient val sc: StreamHiveContext)
  extends LeafNode
  with HiveInspectors {

  override val sparkPlan: SparkPlan = null

  @transient
  val streamTableReader = new CommonStreamTableReader(relation.tableDesc, sc)

  /**
   * The hive object inspector for this table, which can be used to extract values from the
   * serialized row representation.
   */
  @transient
  lazy val objectInspector =
    relation.tableDesc.getDeserializer.getObjectInspector.asInstanceOf[StructObjectInspector]

  /**
   * Functions that extract the requested attributes from the hive output.  Partitioned values are
   * casted from string to its declared data type.
   */
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
  def inputDStream: DStream[_] = streamTableReader.makeDStreamForTable(relation.hiveQlTable)

  def execute() = {
    inputDStream.map { row =>
      val values = attributeFunctions.map(_(row))
      new GenericRow(values.map {
        case n: String if n.toLowerCase == "null" => null
        case varchar: org.apache.hadoop.hive.common.`type`.HiveVarchar => varchar.getValue
        case decimal: org.apache.hadoop.hive.common.`type`.HiveDecimal =>
          BigDecimal(decimal.bigDecimalValue)
        case other => other
      }.toArray)
    }
  }

  def output = attributes
}
