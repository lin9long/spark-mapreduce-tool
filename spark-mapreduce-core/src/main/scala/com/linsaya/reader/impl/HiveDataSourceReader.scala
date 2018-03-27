package com.linsaya.reader.impl

import com.linsaya.SparkStatisticsJob
import com.linsaya.common.util.LoggerUtil
import com.linsaya.reader.SourceReader
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * ${DESCRIPTION}
  *
  * @author llz
  * @create 2018-03-18 9:30
  **/
class HiveDataSourceReader extends SourceReader with LoggerUtil {

   def readDataSource(sc: SparkContext, sqlContext: SQLContext, hiveCtx: HiveContext, rdbProp: IndexedSeq[SparkStatisticsJob.RDBSQLProp]): Unit = {
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
       //      val sql = s"select * from ${prop.tmpTableNameInSpark}"
       //      info(s"execute sql is $sql")
       //      hiveCtx.sql(s"select * from ${prop.tmpTableNameInSpark}").show()
     }
   }
}
