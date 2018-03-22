package com.linsaya.reader

import com.linsaya.SparkStatisticsJob
import com.linsaya.common.util.LoggerUtil
import com.linsaya.conf.SparkConfHolder
import org.apache.spark.sql.DataFrame

/**
  * ${DESCRIPTION}
  *
  * @author llz
  * @create 2018-03-18 9:30
  **/
trait HiveDataSourceReader extends LoggerUtil with SparkConfHolder {
  def readHiveSource(rdbProp: IndexedSeq[SparkStatisticsJob.DataSourceSQLProp]) = {
    for (prop <- rdbProp) {
      info(s"start load connectionProperties table name is ${prop.sourceTableName}")
      var dataframe: DataFrame = null
      info(s"HiveDataSourceReader sql is ${prop.sql}")
      dataframe = hiveCtx.sql(if (prop.sql.isEmpty) {
        error("please set HiveDataSourceReader sql")
        ""
      } else prop.sql)
      info(s"dataframe registerTempTable name is ${prop.tmpTableNameInSpark} count is ${dataframe.count()}")
      dataframe.registerTempTable(prop.tmpTableNameInSpark.toString)
      val sql = s"select * from ${prop.tmpTableNameInSpark}"
      info(s"execute sql is $sql")
      hiveCtx.sql(s"select * from ${prop.tmpTableNameInSpark}").show()
    }
  }
}
