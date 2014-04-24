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

import org.apache.hadoop.hive.metastore.TableType
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.serde.serdeConstants

import org.apache.spark.sql.hive.{StreamHiveContext, HiveMetastoreCatalog}

class StreamMetastoreCatalog (hive: StreamHiveContext)
  extends HiveMetastoreCatalog(hive.hiveContext){
  def dropStream(dropStreamDesc: DropStreamDesc) = {
    val (dbName, streamName) = {
      val splits = dropStreamDesc.getTableName.split("\\.")
      if (splits.length == 1) {
        (SessionState.get().getCurrentDatabase, splits(0))
      } else if (splits.length == 2) {
        (splits(0), splits(1))
      } else {
        throw new IllegalArgumentException("Illegal database and stream name")
      }
    }

    client.dropTable(dbName, streamName, true, !dropStreamDesc.getIfExists)
  }

  //namespace decoration
  def createStream(createStreamDesc: CreateStreamDesc) = {
    val tbl = client.newTable(createStreamDesc.getTableName)

    // Common properties
    Option(createStreamDesc.getDatabaseName).foreach(tbl.setDbName(_))
    Option(createStreamDesc.getTblProps).foreach(tbl.getTTable.getParameters.putAll(_))
    Option(createStreamDesc.getCols).foreach(tbl.setFields(_))
    Option(createStreamDesc.getComment).foreach(tbl.setProperty("comment", _))

    //Serde related
    Option(createStreamDesc.getSerName).map { tbl.setSerializationLib(_) }.getOrElse {
      tbl.setSerializationLib(classOf[org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe].getName)
    }
    Option(createStreamDesc.getFieldDelim).foreach { f =>
      tbl.setSerdeParam(serdeConstants.FIELD_DELIM, f)
      tbl.setSerdeParam(serdeConstants.SERIALIZATION_FORMAT, f)
    }
    Option(createStreamDesc.getFieldEscape).foreach(tbl.setSerdeParam(serdeConstants.ESCAPE_CHAR,_))
    Option(createStreamDesc.getCollItemDelim).foreach(
      tbl.setSerdeParam(serdeConstants.COLLECTION_DELIM, _))
    Option(createStreamDesc.getMapKeyDelim).foreach(
      tbl.setSerdeParam(serdeConstants.MAPKEY_DELIM, _))
    Option(createStreamDesc.getLineDelim).foreach(tbl.setSerdeParam(serdeConstants.LINE_DELIM, _))
    Option(createStreamDesc.getSerdeProps).foreach { props =>
      import scala.collection.JavaConversions._
      props.foreach { case (k, v) => tbl.setSerdeParam(k, v) }
    }

    // input and output format
    Option(createStreamDesc.getInputFormat).foreach { input =>
      tbl.setInputFormatClass(input)
      tbl.getTTable.getSd.setInputFormat(tbl.getInputFormatClass.getName)
    }
    Option(createStreamDesc.getOutputFormat).foreach { output =>
      tbl.setOutputFormatClass(output)
      tbl.getTTable.getSd.setOutputFormat(tbl.getOutputFormatClass.getName)
    }

    if(createStreamDesc.isExternal) {
      tbl.setProperty("EXTERNAL", "TRUE")
      tbl.setTableType(TableType.EXTERNAL_TABLE)
    }

    tbl.setOwner(SessionState.get().getAuthenticator().getUserName())
    tbl.setCreateTime((System.currentTimeMillis() / 1000).toInt)

    client.createTable(tbl, createStreamDesc.getIfNotExists)
  }
}

