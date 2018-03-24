package com.linsaya.job.impl

import com.linsaya.SparkStatisticsJob
import com.linsaya.common.util.LoggerUtil
import com.linsaya.job.MapReduceJob
import com.linsaya.reader.{HiveDataSourceReader, RDBSourceReader}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

 class KpiMapReduceJob extends MapReduceJob with LoggerUtil with RDBSourceReader with HiveDataSourceReader{

  override def excuteJob(sc:SparkContext,hiveCtx:HiveContext,sqlContext:SQLContext, kpiStatisticsProps: IndexedSeq[SparkStatisticsJob.KpiStatisticsSQLProp],
                         dataSourceProps: IndexedSeq[SparkStatisticsJob.DataSourceSQLProp], rdbSQLProps: IndexedSeq[SparkStatisticsJob.RDBSQLProp]): Unit = {
    if(!dataSourceProps.isEmpty){
      info(s"hiveSQLProps size is not empty ,size is ${rdbSQLProps.length}")
      readHiveSource(hiveCtx:HiveContext,dataSourceProps)
    }

    if(!rdbSQLProps.isEmpty){
      info(s"rdbSQLProps size is not empty ,size is ${rdbSQLProps.length}")
//      readRDBSource(rdbSQLProps)
    }
  }
}
