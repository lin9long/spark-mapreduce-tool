package com.richstone.mintaka.gemstack.reader.impl

import java.util.Properties

import com.richstone.mintaka.gemstack.common.CustomTransform
import com.richstone.mintaka.gemstack.common.util.{RDBDataframeUtil, SaveTableUtils}
import com.richstone.mintaka.gemstack.manager.CaseClassManager
import com.richstone.mintaka.gemstack.reader.SourceReader
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}


/**
  * @Description:RDB数据源读取工具
  * @param:
  * @return:
  * @author: llz
  * @Date: 2018/3/28
  */
class RDBSourceReader extends SourceReader with RDBDataframeUtil with CaseClassManager with SaveTableUtils {

  /**
    * @Description:读取数据源
    * @param: [sc, sqlContext, hiveCtx]
    * @return: void
    * @author: llz
    * @Date: 2018/3/28
    */
  def readDataSource[A](sc: SparkContext, sqlContext: SQLContext, hiveCtx: HiveContext, indexedSeq: IndexedSeq[A]): Unit = {
    val rdbProp = indexedSeq.asInstanceOf[IndexedSeq[RDBSQLProp]]
    var dataframe: DataFrame = null
    for (prop <- rdbProp) {
      //创建jdbc连接
      info(s"start load connectionProperties table name is ${prop.sourceTableName}")
      val connectionProperties = new Properties()
      connectionProperties.put("user", prop.user)
      connectionProperties.setProperty("driver", prop.driver)
      connectionProperties.put("password", prop.password)
      connectionProperties.put("table", prop.sourceTableName)
      connectionProperties.put("url", prop.url)
      //生成dataframe
      dataframe = createDataFrameFromRDB(sqlContext, connectionProperties, prop.url)
      dataframe.registerTempTable(prop.tmpTableNameInSpark)

      //做一步etl转换
      if (!prop.sql.isEmpty) {
        info(s"dataframe etl sql is ${prop.sql}")
        dataframe = sqlContext.sql(prop.sql)
      }
      //自定义转换
      if (!prop.customTransformBeanName.isEmpty) {
        val className = prop.customTransformBeanName
        val clazz = Class.forName(className)
        info(s"dataframe customTransForm model is ${prop.customTransformBeanName}")
        dataframe = clazz.newInstance().asInstanceOf[CustomTransform].transform(dataframe, sqlContext, sc)
      }
      //注册成临时表
      info(s"dataframe registerTempTable name is ${prop.tmpTableNameInSpark} count is ${dataframe.count()}")
      dataframe.registerTempTable(prop.tmpTableNameInSpark)
      if (!prop.storageLevel.isEmpty && prop.needCacheTable == "Y") {
        info(s"dataframe ${prop.tmpTableNameInSpark}storageLevel is ${prop.storageLevel}")
        dataframe.persist(getStorageLevel(prop.storageLevel))
      }
    }
  }
}
