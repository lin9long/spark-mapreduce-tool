package com.richstone.mintaka.gemstack.manager

import com.richstone.mintaka.gemstack.common.Constants

/**
  * caseclass管理类
  *
  * @author llz
  * @create 2018-03-28 23:10
  **/
trait CaseClassManager extends Constants{


  //hive数据源
  case class DataSourceSQLProp(sqlNo: String, sql: String, storageLevel: String,
                               needCacheTable: String,
                               targetTableNameInDB: String, tmpTableNameInSpark: String,
                               sourceTableName: String, customTransformBeanName: String)
    extends CommonProp(sqlNo,sql,storageLevel,needCacheTable,tmpTableNameInSpark,customTransformBeanName)

  //kpi数据源
  case class KpiStatisticsSQLProp(sqlNo: String, sql: String, storageLevel: String, needCacheTable: String,
                                  targetTableNameInDB: String, targetPathOfHDFS: String, tmpTableNameInSpark: String
                                  , customTransformBeanName: String)
    extends CommonProp(sqlNo,sql,storageLevel,needCacheTable,tmpTableNameInSpark,customTransformBeanName)

  //rdb数据源
  case class RDBSQLProp(sqlNo: String, sql: String, storageLevel: String, needCacheTable: String,
                        driver: String, user: String, password: String, sourceTableName: String, tmpTableNameInSpark: String,
                        url: String, customTransformBeanName: String, partitionPredicates: String)
    extends CommonProp(sqlNo,sql,storageLevel,needCacheTable,tmpTableNameInSpark,customTransformBeanName)

}

class CommonProp(sqlNo: String, sql: String, storageLevel: String, needCacheTable: String,
                 tmpTableNameInSpark: String, customTransformBeanName: String) {
  val sqlNo_ = sqlNo
  val sql_ = sql
  val storageLevel_ = storageLevel
  val needCacheTable_ = needCacheTable
  val tmpTableNameInSpark_ = tmpTableNameInSpark
  val customTransformBeanName_ = customTransformBeanName
}
