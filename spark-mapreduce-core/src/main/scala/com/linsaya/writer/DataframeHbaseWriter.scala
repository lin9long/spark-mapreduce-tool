package com.linsaya.writer

import java.util.Properties

import com.linsaya.common.Constants
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * ${DESCRIPTION}
  *
  * @author llz
  * @create 2018-03-25 20:58
  **/
trait DataframeHbaseWriter extends Constants {

  def writeDataFrameToHbase(tableName: String, dataframe: DataFrame) = {
    val mysqlTuples = rdbProps("mysql")
    val prop = new Properties()
    mysqlTuples.foreach(tuple => prop.setProperty(tuple._1, tuple._2))
    dataframe.write.mode(SaveMode.Append).jdbc(prop.getProperty("url"),s"spark_$tableName",prop)
    //    dataframe.saveToPhoenix(tableName, configuration, Some("master:2181"), Some(""), false)
    //    dataframe.write.format("org.apache.phoenix.spark").
    //      options(Map("table" -> tableName, "zkUrl" -> "master:2181")).mode(SaveMode.Append).save
  }
}
