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


import org.apache.hadoop.hive.ql.plan.{CreateTableLikeDesc, DDLDesc, CreateTableDesc, DropTableDesc}
import org.apache.hadoop.hive.metastore.api.{Table => TTable, Partition => TPartition}
import org.apache.hadoop.hive.ql.metadata.{Hive, Partition, Table}
import java.net.URI
import org.apache.hadoop.hive.metastore.TableType
import org.apache.hadoop.hive.ql.session.SessionState


class StreamSQLMetastoreCatalog (hive: HiveContext, catalog: HiveMetastoreCatalog) extends HiveMetastoreCatalog(hive){
  override val client = catalog.client

  def dropStream(dropTbl: DropTableDesc) = {
    val tbl: Table = client.getTable(dropTbl.getTableName)
    // simply drop the table here
    if(tbl != null && tbl.canDrop) {
       client.dropTable(dropTbl.getTableName)
    }
  }

  //namespace decoration
  def createStream(createStreamDesc: CreateStreamDesc) = {
    val tbl = client.newTable(createStreamDesc.getTableName)

    if(createStreamDesc.getTblProps != null) {
      tbl.getTTable.getParameters.putAll(createStreamDesc.getTblProps)
    }

    if(createStreamDesc.getCols != null) {
      tbl.setFields(createStreamDesc.getCols)
    }

    if(createStreamDesc.getComment != null) {
      tbl.setProperty("comment", createStreamDesc.getComment)
    }

    if(createStreamDesc.getLocation != null) {
      tbl.setDataLocation(new URI(createStreamDesc.getLocation))
    }

    if(createStreamDesc.isExternal) {
      tbl.setProperty("EXTERNAL", "TRUE")
      tbl.setTableType(TableType.EXTERNAL_TABLE)
    }

    //todo
    tbl.setOwner(SessionState.get().getAuthenticator().getUserName())
    tbl.setCreateTime((System.currentTimeMillis()/1000).toInt)

    client.createTable(tbl, createStreamDesc.getIfNotExists)
  }

  //namespace decoration
  def createStreamLike(createStreamLikeDesc: CreateStreamLikeDesc) = {
    val tbl = client.getTable(createStreamLikeDesc.getLikeTableName)
    val newTable = client.newTable(createStreamLikeDesc.getTableName)
    tbl.setDbName(newTable.getDbName)
    tbl.setTableName(newTable.getTableName)

    if(createStreamLikeDesc.getLocation != null) {
      tbl.setDataLocation(new URI(createStreamLikeDesc.getLocation))
    } else {
      tbl.unsetDataLocation()
    }

    val params = tbl.getParameters
    if(createStreamLikeDesc.getTblProps != null) {
      params.putAll(createStreamLikeDesc.getTblProps)
    }

    if(createStreamLikeDesc.isExternal) {
      tbl.setProperty("EXTERNAL", "TRUE")
      tbl.setTableType(TableType.EXTERNAL_TABLE)
    }

    //todo
    tbl.setOwner(SessionState.get().getAuthenticator().getUserName())
    tbl.setCreateTime((System.currentTimeMillis()/1000).toInt)

    client.createTable(tbl, createStreamLikeDesc.getIfNotExists)
  }

}
