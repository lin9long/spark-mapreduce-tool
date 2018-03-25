package com.linsaya.writer

import java.util.Properties

import com.linsaya.common.Constants
import com.linsaya.common.util.LoggerUtil
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * ${DESCRIPTION}
  *
  * @author llz
  * @create 2018-03-25 20:58
  **/
trait DataframeHbaseWriter extends Constants with LoggerUtil{

  def writeDataFrameToHbase(tableName: String, dataframe: DataFrame) = {
    val mysqlTuples = rdbProps("phoenix")
    val prop = new Properties()
    mysqlTuples.foreach(tuple => prop.setProperty(tuple._1, tuple._2))
    val url = prop.getProperty("url")
    val conn = JdbcUtils.createConnectionFactory(url, prop)()
    val schema = JdbcUtils.schemaString(dataframe, url)
    val schemareplace = schema.replaceAll("TEXT", "VARCHAR").replaceAll("NOT NULL","").replaceFirst(",","primary key,")
    val sql = s"CREATE TABLE spark_$tableName ($schemareplace)"
    info(s"writeDataFrameToHbase sql is $sql")
    val statement = conn.createStatement()
    try {
      statement.executeUpdate(sql)
    } finally {
      statement.close()
    }
    dataframe.write.mode(SaveMode.Append).jdbc(url, s"spark_$tableName", prop)
    //    dataframe.saveToPhoenix(tableName, configuration, Some("master:2181"), Some(""), false)
    //    dataframe.write.format("org.apache.phoenix.spark").
    //      options(Map("table" -> tableName, "zkUrl" -> "master:2181")).mode(SaveMode.Append).save
  }
}
