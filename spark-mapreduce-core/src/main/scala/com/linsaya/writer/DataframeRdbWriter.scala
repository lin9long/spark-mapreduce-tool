package com.linsaya.writer

import java.util.Properties

import com.linsaya.common.Constants
import com.linsaya.common.util.LoggerUtil
import com.linsaya.manager.PropFileManager
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * ${DESCRIPTION}
  *
  * @author llz
  * @create 2018-03-25 20:58
  **/
trait DataframeRdbWriter extends Constants with LoggerUtil with PropFileManager {

  def writeDataFrameToRdb(propName:String ,tableName: String, df: DataFrame) = {
    val mysqlTuples = rdbProps(propName)
    val prop = new Properties()
    mysqlTuples.foreach(tuple => prop.setProperty(tuple._1, tuple._2))
    val url = prop.getProperty("url")
    //    val conn = JdbcUtils.createConnectionFactory(url, prop)()
    //    val schema = JdbcUtils.schemaString(dataframe, url)
    //    val schemareplace = schema.replaceAll("TEXT", "VARCHAR").replaceAll("NOT NULL","").replaceFirst(",","primary key,")
    //    val sql = s"CREATE TABLE spark_$tableName ($schemareplace)"
    //    info(s"writeDataFrameToHbase sql is $sql")
    //    val statement = conn.createStatement()
    //    try {
    //      statement.executeUpdate(sql)
    //    } finally {
    //      statement.close()
    //    }
    //使用不走shuffle的coalesce分区方式，对DataFrame重新分区(分区数为executor个数)，
    //分区的目的：避免输出数据时因分区数太多导致小文件数据太多
    val partition = df.sqlContext.sparkContext.getConf.get("spark.executor.instances", getDefaultPartition).toInt
    df.coalesce(partition).write.mode(SaveMode.Append).jdbc(url, tableName, prop)
    //    dataframe.saveToPhoenix(tableName, configuration, Some("master:2181"), Some(""), false)
    //    dataframe.write.format("org.apache.phoenix.spark").
    //      options(Map("table" -> tableName, "zkUrl" -> "master:2181")).mode(SaveMode.Append).save
  }
}
