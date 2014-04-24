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

package org.apache.spark.sql.hive.stream

import java.util.{ArrayList => JArrayList, HashMap => JHashMap}

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.ql.lib.Node
import org.apache.hadoop.hive.ql.parse._

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.hive.HiveQl
import org.apache.spark.sql.hive.HiveQl.{ParseException, Token}

object StreamQl {
  //all supported TOK
  val streamDdlCommands = Seq(
    //TODO. alter stream related
    "TOK_ALTERSTREAM_PROPERTIES",
    "TOK_ALTERSTREAM_RENAME",
    "TOK_ALTERSTREAM_TOUCH",
    "TOK_ALTERSTREAM_SERDEPROPERTIES",
    "TOK_ALTERSTREAM_SERIALIZER",
    "TOK_ALTERSTREAM_FILEFORMAT",
    "TOK_ALTERSTREAM_LOCATION",

    //TODO. alter column related
    "TOK_ALTERSTREAM_RENAMECOL",
    "TOK_ALTERSTREAM_REPLACECOLS",
    "TOK_ALTERSTREAM_ADDCOLS",
    "TOK_ALTERSTREAM_CHANGECOL_AFTER_POSITION",

    //create stream related
    "TOK_CREATESTREAM",

    //drop stream related
    "TOK_DROPSTREAM"
  )

  def parseSql(sql: String): LogicalPlan = {
    try {
      HiveQl.parseSql(sql)
    } catch {
      case _: Throwable =>
        try {
          val tree = HiveQl.getAst(sql)
          if (streamDdlCommands contains tree.getText) {
            nodeToPlan(tree)
          } else {
            throw new NotImplementedError(sql)
          }
        } catch {
          case e: Throwable => throw new ParseException(sql, e)
        }
    }
  }

  def nodeToPlan(node: Node): LogicalPlan = node match {
    case Token("TOK_CREATESTREAM", children) =>
      val (
        Some(streamNameParts) ::
        ifNotExisted ::
        cols ::
        comment ::
        location ::
        rowFormat ::
        tableProperties ::
        serializer ::
        streamFormat ::
        _ ::
        notImplemented) =
      HiveQl.getClauses(
        Seq(
          "TOK_TABNAME",
          "TOK_IFNOTEXISTS",
          "TOK_TABCOLLIST",
          "TOK_TABLECOMMENT",
          "TOK_TABLELOCATION",
          "TOK_TABLEROWFORMAT",
          "TOK_TABLEPROPERTIES",
          "TOK_TABLESERIALIZER",
          "TOK_TABLEFILEFORMAT",
          "TOK_LIKESTREAM",
          "TOK_QUERY",
          "TOK_STORAGEHANDLER",
          "TOK_TABLEBUCKETS",
          "TOK_TABLEPARTCOLS",
          "TOK_FILEFORMAT_GENERIC",
          "TOK_TABLESKEWED",
          "TOK_TBLSEQUENCEFILE",
          "TOK_TBLTEXTFILE",
          "TOK_TBLRCFILE",
          "TOK_TBLORCFILE"),
        children)
      if (notImplemented.exists(!_.isEmpty)) {
        throw new NotImplementedError(
          s"Unhandled clauses: ${notImplemented.flatten.map(HiveQl.dumpTree(_)).mkString("\n")}")
      }

      val streamName = streamNameParts.getChildren.map { case Token(part, Nil) =>
        HiveQl.cleanIdentifier(part)
      } match {
        case Seq(streamOnly) => streamOnly
        case Seq(dbName, streamName) => s"${dbName}.${streamName}"
      }

      val createStreamDesc = new CreateStreamDesc
      createStreamDesc.setTableName(streamName)
      createStreamDesc.setIfNotExists(ifNotExisted.isDefined)

      comment.foreach { case Token("TOK_TABLECOMMENT", Token(c, Nil) :: Nil) =>
        createStreamDesc.setComment(BaseSemanticAnalyzer.unescapeSQLString(c))
      }

      cols.foreach { node =>
        val colSchemas = BaseSemanticAnalyzer.getColumns(node.asInstanceOf[ASTNode], true)
        createStreamDesc.setCols(colSchemas.asInstanceOf[JArrayList[FieldSchema]])
      }

      val tblProps = new JHashMap[String, String]()
      tableProperties.map { node =>
        BaseSemanticAnalyzer.readProps(node.asInstanceOf[ASTNode], tblProps)
      }

      // Parse the stream location, instead to put into table,
      // here putting into table properties, this implementation is to avoid scheme validation in
      // Hive.
      location.foreach { case Token("TOK_TABLELOCATION", Token(c, Nil) :: Nil) =>
        tblProps.put(CreateStreamDesc.STREAM_LOCATION, BaseSemanticAnalyzer.unescapeSQLString(c))
      }

      createStreamDesc.setTblProps(tblProps)

      rowFormat.foreach {
        case Token("TOK_TABLEROWFORMAT",
               Token("TOK_SERDEPROPS", children) :: Nil) =>

        val (
          rowFormatField ::
          rowFormatCollItems ::
          rowFormatMapKeys ::
          rowFormatLines :: Nil) =
        HiveQl.getClauses(
          Seq(
            "TOK_TABLEROWFORMATFIELD",
            "TOK_TABLEROWFORMATCOLLITEMS",
            "TOK_TABLEROWFORMATMAPKEYS",
            "TOK_TABLEROWFORMATLINES"),
         children
        )

        rowFormatField.foreach { node =>
          val delims = node.getChildren().map { case Token(part, Nil) =>
            BaseSemanticAnalyzer.unescapeSQLString(part)
          }
          createStreamDesc.setFieldDelim(delims(0))
          if (delims.length == 2) createStreamDesc.setFieldEscape(delims(1))
        }

        rowFormatCollItems.foreach {
          case Token("TOK_TABLEROWFORMATCOLLITEMS", Token(part, Nil) :: Nil) =>
            createStreamDesc.setCollItemDelim(BaseSemanticAnalyzer.unescapeSQLString(part))
        }

        rowFormatMapKeys.foreach {
          case Token("TOK_TABLEROWFORMATMAPKEYS", Token(part, Nil) :: Nil) =>
            createStreamDesc.setMapKeyDelim(BaseSemanticAnalyzer.unescapeSQLString(part))
        }

        rowFormatLines.foreach {
          case Token("TOK_TABLEROWFORMATLINES", Token(part, Nil) :: Nil) =>
            createStreamDesc.setLineDelim(BaseSemanticAnalyzer.unescapeSQLString(part))
        }
      }

      serializer.foreach {
        case Token("TOK_TABLESERIALIZER",
                Token("TOK_SERDENAME",
                  Token(serdeName, Nil) :: Token("TOK_TABLEPROPERTIES",
                    Token("TOK_TABLEPROPLIST", children) :: Nil) :: Nil) :: Nil) =>

          createStreamDesc.setSerName(BaseSemanticAnalyzer.unescapeSQLString(serdeName))

          val serdeProps = new JHashMap[String, String]()
          children.foreach {
            case Token("TOK_TABLEPROPERTY",
                    Token(key, Nil) :: Token(value, Nil) :: Nil) =>
              serdeProps.put(
                BaseSemanticAnalyzer.unescapeSQLString(key),
                BaseSemanticAnalyzer.unescapeSQLString(value))
          }
          createStreamDesc.setSerdeProps(serdeProps)
      }

      streamFormat.foreach {
        case Token("TOK_TABLEFILEFORMAT",
                Token(input, Nil):: Token(output, Nil) :: Nil) =>
          createStreamDesc.setInputFormat(BaseSemanticAnalyzer.unescapeSQLString(input))
          createStreamDesc.setOutputFormat(BaseSemanticAnalyzer.unescapeSQLString(output))
      }

      createStreamDesc.setExternal(true)
      createStreamDesc.getTblProps.put(CreateStreamDesc.STREAM_CONSTANTS._1,
        CreateStreamDesc.STREAM_CONSTANTS._2)

    CreateStream(createStreamDesc)

    case Token("TOK_DROPSTREAM", children) =>
      val (Some(streamNameParts) :: ifExisted :: Nil) =
        HiveQl.getClauses(Seq("TOK_TABNAME", "TOK_IFEXISTS"), children)

      val streamName = streamNameParts.getChildren.map { case Token(part, Nil) =>
        HiveQl.cleanIdentifier(part)
      } match {
        case Seq(streamOnly) => streamOnly
        case Seq(dbName, stream) => s"{$dbName}.{$stream}"
      }

      val dropStreamDesc = new DropStreamDesc()
      dropStreamDesc.setIfExists(ifExisted.isDefined)
      dropStreamDesc.setTableName(streamName)
      dropStreamDesc.setExpectView(false)

      DropStream(dropStreamDesc)

    case _ => throw new NotImplementedError("This is not a ASTNode")
  }

  object CreateStreamType extends Enumeration {
      type CreateStreamType = Value
      val CS, // standard create stream
      CSL,    // create stream like a table or stream
      CSAS    // create stream as select [2]
      = Value
  }
}


