package com.richstone.mintaka.gemstack.writer

import java.util.Properties

import com.richstone.mintaka.gemstack.common.Constants
import com.richstone.mintaka.gemstack.common.util.LoggerUtil
import com.richstone.mintaka.gemstack.manager.PropFileManager
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * dataframe写入到rdb数据库
  *
  * @author llz
  * @create 2018-03-25 20:58
  **/
trait DataframeRdbWriter extends Constants with LoggerUtil with PropFileManager {

  def writeDataFrameToRdb(propName:String ,tableName: String, df: DataFrame) = {
    val prop: Properties = genConnProp(propName)
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
    import scala.collection.mutable._
    val arr = Set(1,2,3)
    arr += 2
    //使用不走shuffle的coalesce分区方式，对DataFrame重新分区(分区数为executor个数)，
    //分区的目的：避免输出数据时因分区数太多导致小文件数据太多
    val partition = df.sqlContext.sparkContext.getConf.get("spark.executor.instances", getDefaultPartition).toInt
    df.coalesce(partition).write.mode(SaveMode.Append).jdbc(url, tableName, prop)
    //    dataframe.saveToPhoenix(tableName, configuration, Some("master:2181"), Some(""), false)
    //    dataframe.write.format("org.apache.phoenix.spark").
    //      options(Map("table" -> tableName, "zkUrl" -> "master:2181")).mode(SaveMode.Append).save
  }

  private def genConnProp(propName: String) = {
    val connTuples = rdbProps(propName)
    val prop = new Properties()
    val sysFile = getSysPropertiesFile
    val url = sysFile.getProperty("jdbc.url","")
    val driver =sysFile.getProperty("jdbc.driver","")
    val user =sysFile.getProperty("jdbc.user","")
    val password =sysFile.getProperty("jdbc.password","")
    connTuples.foreach(tuple => prop.setProperty(tuple._1, tuple._2))
    prop
  }
}
