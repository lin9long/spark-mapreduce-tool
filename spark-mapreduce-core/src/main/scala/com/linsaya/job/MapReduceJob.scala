package com.linsaya.job

import com.linsaya.SparkStatisticsJob
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

abstract trait MapReduceJob {
  def excuteJob(sc:SparkContext,hiveCtx:HiveContext,sqlContext:SQLContext, kpiStatisticsProps: IndexedSeq[SparkStatisticsJob.KpiStatisticsSQLProp],
                dataSourceProps: IndexedSeq[SparkStatisticsJob.DataSourceSQLProp], RDBprops: IndexedSeq[SparkStatisticsJob.RDBSQLProp])
}
