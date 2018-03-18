package com.linsaya.reader

import com.linsaya.SparkStatisticsJob
import com.linsaya.common.util.LoggerUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * ${DESCRIPTION}
  *
  * @author llz
  * @create 2018-03-18 9:30
  **/
trait HiveDataSourceReader extends LoggerUtil {
  def readHiveSource(sc: SparkContext, sqlContext: SQLContext, rdbProp: IndexedSeq[SparkStatisticsJob.DataSourceSQLProp]) = {
    for (prop <- rdbProp) {
      info(s"start load connectionProperties table name is ${prop.sourceTableName}")
      val hiveContext = new HiveContext(sc)
      //      val connectionProperties = new Properties()
      //      connectionProperties.put("user", prop.user)
      //      connectionProperties.setProperty("driver", prop.driver)
      //      connectionProperties.put("password", prop.password)
      //      connectionProperties.put("table", prop.sourceTableName)
      //      connectionProperties.put("url", prop.url)

      var dataframe: DataFrame = null
      info(s"HiveDataSourceReader sql is ${prop.sql}")
      dataframe = hiveContext.sql(if (prop.sql.isEmpty) {
        error("please set HiveDataSourceReader sql")
        prop.sql
      } else "")
      dataframe.registerTempTable(prop.tmpTableNameInSpark)

      dataframe.show()

      //      if (!prop.sql.isEmpty) {
      //        dataframe = sqlContext.sql(prop.sql)
      //      }

      //      if (!prop.customTransForm.isEmpty) {
      //        val className = prop.customTransForm
      //        val clazz = Class.forName(className)
      //        dataframe = clazz.newInstance().asInstanceOf[CustomTransform].transform(dataframe, sqlContext, sc)
      //      }

      //      dataframe.registerTempTable(prop.tmpTableNameInSpark)
    }
  }
}
