package com.linsaya.reader.impl

import java.util.Properties

import com.linsaya.common.CustomTransform
import com.linsaya.common.util.{RDBDataframeUtil, SaveTableUtils}
import com.linsaya.manager.RDBPropManager
import com.linsaya.reader.SourceReader
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}


class RDBSourceReader extends SourceReader with RDBDataframeUtil
  with RDBPropManager with SaveTableUtils {

  def readDataSource(sc: SparkContext, sqlContext: SQLContext, hiveCtx: HiveContext): Unit = {
     val rdbProp = genRDBSQLProp(getSysPropertiesFile)
     var dataframe: DataFrame = null
     for (prop <- rdbProp) {
       info(s"start load connectionProperties table name is ${prop.sourceTableName}")
       val connectionProperties = new Properties()
       connectionProperties.put("user", prop.user)
       connectionProperties.setProperty("driver", prop.driver)
       connectionProperties.put("password", prop.password)
       connectionProperties.put("table", prop.sourceTableName)
       connectionProperties.put("url", prop.url)

       dataframe = createDataFrameFromRDB(sqlContext, connectionProperties, prop.url)
       dataframe.registerTempTable(prop.tmpTableNameInSpark)

       if (!prop.sql.isEmpty) {
         info(s"dataframe etl sql is ${prop.sql}")
         dataframe = sqlContext.sql(prop.sql)
       }

       if (!prop.customTransForm.isEmpty) {
         val className = prop.customTransForm
         val clazz = Class.forName(className)
         info(s"dataframe customTransForm model is ${prop.customTransForm}")
         dataframe = clazz.newInstance().asInstanceOf[CustomTransform].transform(dataframe, sqlContext, sc)
       }

       info(s"dataframe registerTempTable name is ${prop.tmpTableNameInSpark} count is ${dataframe.count()}")
       dataframe.registerTempTable(prop.tmpTableNameInSpark)
       if (!prop.storageLevel.isEmpty && prop.needCacheTable == "Y") {
         info(s"dataframe ${prop.tmpTableNameInSpark}storageLevel is ${prop.storageLevel}")
         dataframe.persist(getStorageLevel(prop.storageLevel))
       }
     }
   }
}
