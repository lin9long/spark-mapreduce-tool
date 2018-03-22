package com.linsaya.job

import com.linsaya.SparkStatisticsJob

abstract trait MapReduceJob {
  def excuteJob( kpiStatisticsProps: IndexedSeq[SparkStatisticsJob.KpiStatisticsSQLProp],
                dataSourceProps: IndexedSeq[SparkStatisticsJob.DataSourceSQLProp], RDBprops: IndexedSeq[SparkStatisticsJob.RDBSQLProp])
}
