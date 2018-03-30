package com.richstone.mintaka.gemstack.common.util

import java.sql.ResultSet
import java.util.Properties

import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * @Description:  rdbdataframe工具类
  * @author llz
  * @date 2018/3/1610:47
  */
trait RdbDataframeUtil {

  /**
    * @Description: 通过表名获取RDB的Schema
    * @param: [connectionProperties, sql]
    * @return: Unit
    * @author: llz
    * @Date: 2018/3/18
    */
  def getRDBSchemaByTableName(conn: Properties, sql: String): ArrayBuffer[Tuple2[String, String]] = {
    //    df2.write.mode(SaveMode.Append).jdbc("jdbc:oracle:thin:@192.168.6.98:1521:xe", "TEST", connectionProperties)
    //    df1.write.mode(SaveMode.Append).jdbc("jdbc:oracle:thin:@192.168.6.98:1521:xe", "F_CZ_GEMSTACK_APP_KPI_H", connectionProperties)
    import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
    val url = conn.getProperty("url","")
    val connection = JdbcUtils.createConnectionFactory(url, conn).apply()
    //    val result = connection.createStatement().executeQuery(sql)
    val schema = connection.getMetaData
    val columnSet = schema.getColumns(null, null, conn.getProperty("table"), "%")
    import scala.collection.mutable.ArrayBuffer
    val schemaVaule = ArrayBuffer[Tuple2[String, String]]()
    while (columnSet.next()) {
      schemaVaule += (Tuple2(columnSet.getString("COLUMN_NAME"),
        columnSet.getString("TYPE_NAME")))
    }
    schemaVaule
  }

  /**
    * @Description: 通过sql语句执行结果获取RDBSchema
    * @param: [connectionProperties, result]
    * @return: _root_.scala.collection.mutable.ArrayBuffer[(_root_.scala.Predef.String, _root_.scala.Predef.String)]
    * @author: llz
    * @Date: 2018/3/18
    */
  def getRDBSchemaBySqlResult(connectionProperties: Properties, result: ResultSet): (ArrayBuffer[Tuple2[String, String]]) = {
    val url = connectionProperties.getProperty("url")
    //    val connection = JdbcUtils.createConnectionFactory(url, connectionProperties).apply()
    //    val result = connection.createStatement().executeQuery(sql)
    val metadata = result.getMetaData
    val count = metadata.getColumnCount
    val schemaVaule = ArrayBuffer[Tuple2[String, String]]()
    for (i <- 1 to count) {
      schemaVaule += (Tuple2(metadata.getColumnName(i), metadata.getColumnTypeName(i)))
    }
    schemaVaule
  }

  /**
    * @Description: 通过sql获取Resultset
    * @param: [connectionProperties, sql]
    * @return: _root_.java.sql.ResultSet
    * @author: llz
    * @Date: 2018/3/18
    */
  def getResultSetBySql(conn: Properties, sql: String): ResultSet = {
    val connection = JdbcUtils.createConnectionFactory(conn.getProperty("url"), conn).apply()
    connection.createStatement().executeQuery(sql)
  }

  /**
    * @Description: 将Resultset转换成list集合
    * @param: [res]
    * @return: _root_.scala.collection.mutable.ListBuffer[_root_.scala.collection.mutable.ArrayBuffer[_root_.scala.Predef.String]
    * @author: llz
    * @Date: 2018/3/18
    */
  def convertResultSetToList(res: ResultSet): ListBuffer[ArrayBuffer[String]] = {
    var res_list = ListBuffer[ArrayBuffer[String]]()
    while (res.next()) {
      var i = 1
      var res_list_value = ArrayBuffer[String]()
      while (i <= res.getMetaData.getColumnCount) {
        val r = res.getString(i)
        res_list_value = if (r.isInstanceOf[String]) res_list_value :+ r else res_list_value :+ ""
        i += 1
      }
      res_list += res_list_value
    }
    res_list
  }

  /**
    * @Description:从rdb数据库中读取dataframe
    * @param: [sqlContext, properties, url]
    * @return: _root_.org.apache.spark.sql.DataFrame
    * @author: llz
    * @Date: 2018/3/17
    */
  def createDataFrameFromRdb(sqlContext: SQLContext, properties: Properties, url: String): DataFrame = {
    sqlContext.read.jdbc(url, properties.getProperty("sourceTableName"), properties)
  }
  /**
    * @Description:从rdb数据库中读取dataframe,根据Predicates分区
    * @param: [sqlContext, properties, url]
    * @return: _root_.org.apache.spark.sql.DataFrame
    * @author: llz
    * @Date: 2018/3/17
    */
  def createDataFrameFromRdbByPredicates(sqlContext: SQLContext,predicates :Array[String], properties: Properties, url: String): DataFrame = {
    sqlContext.read.jdbc(url, properties.getProperty("sourceTableName"), predicates,properties)
  }
}
