package com.linsaya.manager

import com.linsaya.SparkStatisticsJob.getPropertiesFile
import com.linsaya.common.util.LoggerUtil

trait RDBPropManager extends LoggerUtil {

  case class RDBSQLProp(sqlNo: String, sql: String, storageLevel: String, needCacheTable: String,
                        driver: String, user: String, password: String, sourceTableName: String, tmpTableNameInSpark: String,
                        url: String,customTransForm:String)

  def genRDBSQLProp(paths: Array[String]): IndexedSeq[RDBSQLProp] = {
    if (paths.isEmpty) {
      error("DataSourceSQLProp is empty")
    }
    val props = for (i <- 0 until paths.length) yield getPropertiesFile(paths(i))
    val propertieses = props.sortBy(p => p.getProperty("sqlNo"))
    val propList = for (prop <- propertieses) yield (new RDBSQLProp(prop.getProperty("sqlNo",""),
      prop.getProperty("sql",""), prop.getProperty("storageLevel",""), prop.getProperty("needCacheTable",""),
      prop.getProperty("driver",""), prop.getProperty("user",""), prop.getProperty("password",""),
      prop.getProperty("sourceTableName",""), prop.getProperty("tmpTableNameInSpark",""), prop.getProperty("url",""),
      prop.getProperty("customTransForm","")
    ))
    propList.seq
  }
}
