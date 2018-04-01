package com.richstone.mintaka.gemstack.manager

import java.util.Properties

import com.richstone.mintaka.gemstack.common.util.LoggerUtil

/**
  * @Description: kpi数据源管理类
  * @param:
  * @return:
  * @author: llz
  * @Date: 2018/3/28
  */
trait KpiStatisticsPropManager extends LoggerUtil with PropFileManager with CaseClassManager {


  def genKpiStatisticsProp(appPropFile: Properties): IndexedSeq[KpiStatisticsSQLProp] = {
    val paths = if (appPropFile.getProperty(kpi_statistics_sql_file_paths).isEmpty) return null
    else appPropFile.getProperty(kpi_statistics_sql_file_paths).split(",")
    if (paths.isEmpty) {
      error("KpiStatisticsSQLProp is empty")
    }
    val props = for (i <- 0 until paths.length) yield getPropertiesFile(paths(i))
    val propertieses = props.sortBy(p => p.getProperty("sqlNo"))
    val propList = for (prop <- propertieses) yield new KpiStatisticsSQLProp(prop.getProperty("sqlNo", ""),
      prop.getProperty("sql", ""), prop.getProperty("storageLevel", ""), prop.getProperty("needCacheTable", ""),
      prop.getProperty("targetTableNameInDB", ""), prop.getProperty("targetPathOfHDFS", ""),
      prop.getProperty("tmpTableNameInSpark", ""), prop.getProperty("customTransformBeanName", "")
    )
    propList.seq
  }
}
