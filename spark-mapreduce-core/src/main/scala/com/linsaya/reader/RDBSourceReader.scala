package com.linsaya.reader

import java.util.Properties

import com.linsaya.SparkStatisticsJob
import com.linsaya.common.CustomTransform
import com.linsaya.common.util.{LoggerUtil, RDBDataframeUtil}
import com.linsaya.conf.SparkConfHolder
import org.apache.spark.sql.DataFrame


trait RDBSourceReader extends RDBDataframeUtil with LoggerUtil with SparkConfHolder{

  def readRDBSource(rdbProp: IndexedSeq[SparkStatisticsJob.RDBSQLProp]) = {
    for (prop <- rdbProp) {
      info(s"start load connectionProperties table name is ${prop.sourceTableName}")
      val connectionProperties = new Properties()
      connectionProperties.put("user", prop.user)
      connectionProperties.setProperty("driver", prop.driver)
      connectionProperties.put("password", prop.password)
      connectionProperties.put("table", prop.sourceTableName)
      connectionProperties.put("url", prop.url)

      var dataframe: DataFrame = null
      dataframe = createDataFrameFromRDB(sqlContext, connectionProperties, prop.url)
      dataframe.registerTempTable(prop.tmpTableNameInSpark)

      if (!prop.sql.isEmpty) {
        info(s"dataframe etl sql is ${prop.sql}")
        dataframe = sqlContext.sql(prop.sql)
      }

      if (!prop.customTransForm.isEmpty) {
        val className = prop.customTransForm
        val clazz = Class.forName(className)
        info(s"dataframe customTransForm model is  ${prop.customTransForm}")
        dataframe = clazz.newInstance().asInstanceOf[CustomTransform].transform(dataframe, sqlContext, sc)
      }
      info(s"dataframe registerTempTable name is ${prop.tmpTableNameInSpark} count is ${dataframe.count()}")
      dataframe.registerTempTable(prop.tmpTableNameInSpark)
    }
  }
}
