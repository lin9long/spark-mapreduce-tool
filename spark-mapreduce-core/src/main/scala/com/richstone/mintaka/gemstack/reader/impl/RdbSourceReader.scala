package com.richstone.mintaka.gemstack.reader.impl

import java.util.Properties

import com.richstone.mintaka.gemstack.common.CustomTransform
import com.richstone.mintaka.gemstack.common.util.{RdbDataframeUtil, SaveTableUtil}
import com.richstone.mintaka.gemstack.manager.CaseClassManager
import com.richstone.mintaka.gemstack.reader.SourceReader
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}


/**
  * @Description:RDB数据源读取工具
  * @param:
  * @return:
  * @author: llz
  * @Date: 2018/3/28
  */
class RdbSourceReader extends SourceReader with RdbDataframeUtil with CaseClassManager with SaveTableUtil {

  /**
    * @Description:读取数据源
    * @param: [sc, sqlContext, hiveCtx]
    * @return: void
    * @author: llz
    * @Date: 2018/3/28
    */
  def readDataSource[A](sqlContext: SQLContext, hiveCtx: HiveContext, indexedSeq: IndexedSeq[A]): Unit = {
    val rdbProp = indexedSeq.asInstanceOf[IndexedSeq[RDBSQLProp]]
    var dataframe: DataFrame = null
    for (prop <- rdbProp) {
      //创建jdbc连接
      info(s"start load connectionProperties table name is ${prop.sourceTableName}")

      dataframe = loadDfByPredicates(prop, hiveCtx)
      //生成dataframe
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
        dataframe = clazz.newInstance().asInstanceOf[CustomTransform].transform(dataframe, sqlContext, sqlContext.sparkContext)
      }
      //注册成临时表
      info(s"dataframe registerTempTable name is ${prop.tmpTableNameInSpark} count is ${dataframe.count()}")
      registerTempTable(prop.tmpTableNameInSpark, dataframe, hiveCtx, prop.storageLevel)
      //      dataframe.registerTempTable(prop.tmpTableNameInSpark)
      //      if (!prop.storageLevel.isEmpty && prop.needCacheTable == "Y") {
      //        info(s"dataframe ${prop.tmpTableNameInSpark}storageLevel is ${prop.storageLevel}")
      //        dataframe.persist(getStorageLevel(prop.storageLevel))
      //      }
    }
  }

  /**
    * @Description: 创建数据库连接配置
    * @param: [prop]
    * @return: java.util.Properties
    * @author: llz
    * @Date: 2018/3/29
    */
  private def genConnProp[A](prop: RDBSQLProp) = {
    val connectionProperties = new Properties()
    connectionProperties.put("user", prop.user)
    connectionProperties.setProperty("driver", prop.driver)
    connectionProperties.put("password", prop.password)
    connectionProperties.put("sourceTableName", prop.sourceTableName)
    connectionProperties.put("url", prop.url)
    connectionProperties
  }

  /**
    * @Description: 根据分词信息创建dataframe
    * @param: [prop, hiveCtx]
    * @return: void
    * @author: llz
    * @Date: 2018/3/29
    */
  def loadDfByPredicates(prop: RDBSQLProp, hiveCtx: HiveContext): DataFrame = {
    val conn: Properties = genConnProp(prop)
    val predicates = if (!prop.partitionPredicates.isEmpty) prop.partitionPredicates.split(";") else new Array[String](0)
    if (predicates.length > 0) {
      createDataFrameFromRdbByPredicates(hiveCtx, predicates, conn, prop.url)
    } else {
      createDataFrameFromRdb(hiveCtx, conn, prop.url)
    }
  }
}
