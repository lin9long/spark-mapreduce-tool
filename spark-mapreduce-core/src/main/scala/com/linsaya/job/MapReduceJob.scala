package com.linsaya.job

import com.linsaya.SparkStatisticsJob
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

abstract trait MapReduceJob {
  def excuteJob(sc: SparkContext, sQLContext: SQLContext,kpiStatisticsProps: IndexedSeq[SparkStatisticsJob.KpiStatisticsSQLProp],
                dataSourceProps: IndexedSeq[SparkStatisticsJob.DataSourceSQLProp], RDBprops: IndexedSeq[SparkStatisticsJob.RDBSQLProp])
}
