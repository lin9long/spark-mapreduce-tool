package com.richstone.mintaka.gemstack.manager

import java.util.Properties

import com.richstone.mintaka.gemstack.common.util.LoggerUtil

/**
  * @Description: rdb数据源管理类
  * @param:
  * @return:
  * @author: llz
  * @Date: 2018/3/28
  */
trait RdbSourcePropManager extends LoggerUtil with PropFileManager with CaseClassManager {

  def genRDBSQLProp(appPropFile: Properties): IndexedSeq[RDBSQLProp] = {
    val paths = if (appPropFile.getProperty(rdb_data_source_sql_file_paths).isEmpty) return null
    else appPropFile.getProperty(rdb_data_source_sql_file_paths).split(",")
    if (paths.isEmpty) {
      error("DataSourceSQLProp is empty")
    }
    val props = for (i <- 0 until paths.length) yield getPropertiesFile(paths(i))
    val propertieses = props.filter(_!=null).sortBy(p => Integer.valueOf(p.getProperty("sqlNo")))
    val propList = for (prop <- propertieses) yield new RDBSQLProp(prop.getProperty("sqlNo", ""),
      prop.getProperty("sql", ""), prop.getProperty("storageLevel", ""), prop.getProperty("needCacheTable", ""),
      prop.getProperty("driver", ""), prop.getProperty("user", ""), prop.getProperty("password", ""),
      prop.getProperty("sourceTableName", ""), prop.getProperty("tmpTableNameInSpark", ""), (prop.getProperty("url", "")),
      prop.getProperty("customTransformBeanName", ""), prop.getProperty("partitionPredicates", "")
    )
    propList.seq
  }
}
