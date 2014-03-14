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
package hive
package streamsql

import java.util.ArrayList
import java.util.HashMap
import java.util.Map
import java.util.List

import org.apache.hadoop.hive.ql.plan.{CreateTableLikeDesc, DDLDesc, CreateTableDesc, DropTableDesc}
import org.apache.hadoop.hive.ql.parse._
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer

import org.apache.spark.sql.hive.HiveQl

import scala.reflect._

import scala.collection.JavaConversions._

trait BaseCreateStreamDesc extends DDLDesc {
  @BeanProperty
  var streamProp: (String, String) = ("STREAM", "TRUE")
}

class CreateStreamDesc extends CreateTableDesc with BaseCreateStreamDesc {
  super.setExternal(true)
  super.getTblProps.put(streamProp._1, streamProp._2)
}

class CreateStreamLikeDesc extends CreateTableLikeDesc with BaseCreateStreamDesc {
  super.setExternal(true)
  super.getTblProps.put(streamProp._1, streamProp._2)
}

//TODO to update the analyzer based on getClause and Token, instead of hive utils.
object StreamSQLDDLPostParser {
  protected def getUnescapedTableName(node: ASTNode): String = {
    val Some(tableNameParts) = HiveQl.getClauseOption("TOK_TABNAME", node.getChildren)
    val tablename: String =
      tableNameParts.getChildren.map{ case HiveQl.Token(part, Nil) => HiveQl.cleanIdentifier(part)} match {
        case Seq(tableOnly) => tableOnly
        case Seq(databaseName, table) => "%s.%s".format(databaseName, table)
      }
    tablename
  }

  def analyzeDropStreamDesc(node: ASTNode):  DropTableDesc = {
    val dropTableDesc = new DropTableDesc()
    dropTableDesc.setIfExists(HiveQl.getClauseOption("TOK_IFEXISTS", node.getChildren).isDefined)
    //pass the tab or db.tab into table_name field.
    dropTableDesc.setTableName(getUnescapedTableName(node))
    //not supportive
    dropTableDesc.setExpectView(false)
    dropTableDesc
  }


  def analyzerCreateStreamDesc(node: ASTNode): BaseCreateStreamDesc = {
    object CreateStreamType extends Enumeration {
      type CreateStreamType = Value
      val CS, // standard create stream
      CSL,    // create stream like a table or stream
      CSAS    // create stream as select [2]
      = Value
    }

    val tableName = getUnescapedTableName(node)
    var likeTableName: String = null
    var ifNotExists: Boolean = false
    var creatStreamType: CreateStreamType.Value = CreateStreamType.CS
    var cols: List[FieldSchema] = null
    var comment: String = null
    var location: String = null
    val tblProps: Map[String, String] = new HashMap[String, String]

    for (cnode <- node.getChildren ) {
      val child = cnode.asInstanceOf[ASTNode]
      // Use token type instead of text to avoid some up-/low-case mismatch issues.
      child.getText match {
        case "TOK_IFNOTEXISTS" => ifNotExists = true
        case "KW_EXTERNAL" =>
          //do nothing, since we force all stream table as external table,
          //which is beyond the management scope of hive warehouse
        case "TOK_LIKETABLE" =>
          if(child.getChildCount > 0) {
            likeTableName = getUnescapedTableName(child)
            if ( CreateStreamType.CSAS == creatStreamType) {
              // no csas and csl[s|t]
              throw new SemanticException("Create Stream Like Stream/Table cannot co-exist with Create Stream As Stream");
            } else if(cols.size() > 0){
              // no column list definition for csl[s|t]
              throw new SemanticException("No columns' definition in Create Stream Like Stream/Table")
            }
            creatStreamType = CreateStreamType.CSL
          }
        case "TOK_QUERY" =>
          creatStreamType = CreateStreamType.CSAS
          throw new NotImplementedError(s"Not support yet:\n ${HiveQl.dumpTree(child).toString} ")
        case "TOK_TABCOLLIST" =>
          cols = BaseSemanticAnalyzer.getColumns(child, true)
        case "TOK_TABLECOMMENT" =>
          comment = BaseSemanticAnalyzer.unescapeSQLString(child.getChild(0).getText)
        case "TOK_TABLEROWFORMAT" =>
        case "TOK_TABLELOCATION" =>
          location = BaseSemanticAnalyzer.unescapeSQLString(child.getChild(0).getText)
        case "TOK_TABLEPROPERTIES" =>
          //todo check essential properties for stream
          BaseSemanticAnalyzer.readProps(child.getChild(0).asInstanceOf[ASTNode], tblProps)
//        case "TOK_TABLEROWFORMAT" =>
//           //TODO
//        case "TOK_TABLESERIALIZER" =>
//           //TODO
//        case "TOK_STORAGEHANDLER" =>
//           //TODO
//        case "TOK_TABLEFILEFORMAT" =>
//           //TODO input/output class
        case "TOK_TABLEBUCKETS" =>
          throw new NotImplementedError(s"Not support yet:\n ${HiveQl.dumpTree(child).toString} ")
        case "TOK_TABLEPARTCOLS" =>
          throw new NotImplementedError(s"Not support yet:\n ${HiveQl.dumpTree(child).toString} ")
        case "TOK_FILEFORMAT_GENERIC" =>
          throw new SemanticException("Unrecongized file format in STORED AS clause: " + child.getText)
        case "TOK_TABLESKEWED"  =>
          throw new NotImplementedError(s"Not support yet:\n ${HiveQl.dumpTree(child).toString} ")
        case _ =>
          throw new AssertionError("Unknow token: " + child.getToken)
      }
    }

    creatStreamType match {
      case CreateStreamType.CS =>
        val csd = new CreateStreamDesc
        csd.setCols(cols.asInstanceOf[ArrayList[FieldSchema]])
        csd.setComment(comment)
        csd.setTableName(tableName)
        csd.setLocation(location)
        tblProps.foreach(i => csd.getTblProps.put(i._1,i._2))
        csd
      case CreateStreamType.CSL =>
        val csld = new CreateStreamLikeDesc
        csld.setTableName(tableName)
        csld.setLocation(location)
        csld.setLikeTableName(likeTableName)
        tblProps.foreach(i => csld.getTblProps.put(i._1,i._2))
        csld
      case CreateStreamType.CSAS =>
        //todo
        val csd = new CreateStreamDesc
        csd
      case _ =>
        throw new SemanticException("Unsupported command");

    }

  }

}
