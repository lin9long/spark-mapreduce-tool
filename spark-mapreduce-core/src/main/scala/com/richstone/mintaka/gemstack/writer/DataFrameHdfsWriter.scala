package com.richstone.mintaka.gemstack.writer

import com.richstone.mintaka.gemstack.manager.PropFileManager
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * @Description: ${todo}
  * @author llz
  * @date 2018/3/2714:39
  */
trait DataFrameHdfsWriter extends PropFileManager  {
  def writeDataFrameToHdfs(path: String, df: DataFrame) = {
    //使用不走shuffle的coalesce分区方式，对DataFrame重新分区(分区数为executor个数)，
    //分区的目的：避免输出数据时因分区数太多导致小文件数据太多
    val partition = df.sqlContext.sparkContext.getConf.get("spark.executor.instances", getDefaultPartition).toInt
    df.coalesce(partition).write.mode(SaveMode.Append).parquet(path)
  }
}
