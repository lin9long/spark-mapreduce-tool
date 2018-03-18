package com.linsaya.job.impl

import com.linsaya.SparkStatisticsJob
import com.linsaya.job.MapReduceJob
import com.linsaya.job.MapReduceJob
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

class StatisticsMapReduceJob extends MapReduceJob{
  override def excuteJob(sc: SparkContext, sQLContext: SQLContext,kpiStatisticsProps: IndexedSeq[SparkStatisticsJob.KpiStatisticsSQLProp],
                         dataSourceProps: IndexedSeq[SparkStatisticsJob.DataSourceSQLProp], RDBprops: IndexedSeq[SparkStatisticsJob.RDBSQLProp]): Unit = {
    println("excuteJob------->StatisticsMapReduceJob")
  }
}
