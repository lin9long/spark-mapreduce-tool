package com.linsaya.job.impl

import com.linsaya.SparkStatisticsJob
import com.linsaya.job.MapReduceJob

class StatisticsMapReduceJob extends MapReduceJob{
  override def excuteJob( kpiStatisticsProps: IndexedSeq[SparkStatisticsJob.KpiStatisticsSQLProp],
                         dataSourceProps: IndexedSeq[SparkStatisticsJob.DataSourceSQLProp], RDBprops: IndexedSeq[SparkStatisticsJob.RDBSQLProp]): Unit = {
    println("excuteJob------->StatisticsMapReduceJob")
  }
}
