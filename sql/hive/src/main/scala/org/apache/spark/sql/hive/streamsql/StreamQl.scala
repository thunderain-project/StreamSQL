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

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.ql.lib.Node
import org.apache.hadoop.hive.ql.parse._

import org.apache.spark.sql.catalyst.plans.logical._


object StreamQl {

  //all supported TOK
  val streamSqlDdlCommands = Seq(
    //TODO alter stream related
    "TOK_ALTERSTREAM_PROPERTIES",
    "TOK_ALTERSTREAM_RENAME",
    "TOK_ALTERSTREAM_TOUCH",
    "TOK_ALTERSTREAM_SERDEPROPERTIES",
    "TOK_ALTERSTREAM_SERIALIZER",
    "TOK_ALTERSTREAM_FILEFORMAT",
    "TOK_ALTERSTREAM_LOCATION",
    //TODO alter column related
    "TOK_ALTERSTREAM_RENAMECOL",
    "TOK_ALTERSTREAM_REPLACECOLS",
    "TOK_ALTERSTREAM_ADDCOLS",
    "TOK_ALTERSTREAM_CHANGECOL_AFTER_POSITION",

    //create stream related
    "TOK_CREATESTREAM",
    //"TOK_LIKESTREAM",

    //drop stream related
    "TOK_DROPSTREAM"
  )


  def nodeToPlan(node: Node): LogicalPlan = node match {
    case a: ASTNode => (a.getText, a) match {
      case ("TOK_DROPSTREAM", a) =>
        // prepare the parameters into LogicalPlan
        // wrap that LogicalPlan with StreamSQLDDLCommand
        StreamSQLDDLCommand(new DropStream(a))
      case ("TOK_CREATESTREAM", a) =>
        // TODO CTAS ?
        // prepare the parameters into LogicalPlan
        // wrap that LogicalPlan with StreamSQLDDLCommand
        StreamSQLDDLCommand(new DropStream(a))
      case _ =>
        throw new NotImplementedError(s"No parse rules for:\n ${HiveQl.dumpTree(a).toString} ")
    }
    case _ => throw new NotImplementedError("This is not a ASTNode")
  }
}

class DropStream(node: ASTNode) extends StreamSQLDDLPlan {
  def execute(catalog: StreamSQLMetastoreCatalog): Seq[String] = {
    //TODO
    val results = new JArrayList[String]
    val dropStreamDesc = StreamSQLDDLPostParser.analyzeDropStreamDesc(node)
    catalog.dropStream(dropStreamDesc)
    results
  }
}

class CreateStream(node: ASTNode) extends StreamSQLDDLPlan {
  def execute(catalog: StreamSQLMetastoreCatalog): Seq[String] = {
    //TODO
    val results = new JArrayList[String]
    StreamSQLDDLPostParser.analyzeDropStreamDesc(node) match {
      case csd: CreateStreamDesc =>  catalog.createStream(csd)
      case csld: CreateStreamLikeDesc => catalog.createStreamLike(csld)
    }
    results
  }
}

// in case we need to convert to physical plan?
abstract class StreamSQLDDLPlan extends LogicalPlan {
  self: Product =>
  //TODO if it is possible to obtain catalog to Analyzer
  abstract def execute(catalog: StreamSQLMetastoreCatalog) : Seq[String]
}