package com.linsaya.job.impl

import com.linsaya.SparkStatisticsJob
import com.linsaya.common.util.LoggerUtil
import com.linsaya.job.MapReduceJob
import com.linsaya.reader.RDBSourceReader
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

 class KpiMapReduceJob extends MapReduceJob with LoggerUtil with RDBSourceReader{

  override def excuteJob(sc: SparkContext, sqLContext: SQLContext, kpiStatisticsProps: IndexedSeq[SparkStatisticsJob.KpiStatisticsSQLProp],
                         dataSourceProps: IndexedSeq[SparkStatisticsJob.DataSourceSQLProp], rdbSQLProps: IndexedSeq[SparkStatisticsJob.RDBSQLProp]): Unit = {

    if(!rdbSQLProps.isEmpty){
      info(s"rdbSQLProps size is not empty ,size is ${rdbSQLProps.length}")
      readSource(sc,sqLContext,rdbSQLProps)
    }
  }
}
