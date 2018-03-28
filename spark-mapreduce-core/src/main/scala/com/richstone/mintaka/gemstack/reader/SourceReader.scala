package com.richstone.mintaka.gemstack.reader

import com.richstone.mintaka.gemstack.common.util.LoggerUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * @Description: ${todo}
  * @author llz
  * @date 2018/3/279:20
  */
trait SourceReader extends LoggerUtil{
  def readDataSource[A](sc: SparkContext, sqlContext: SQLContext, hiveCtx: HiveContext,indexedSeq: IndexedSeq[A])
}
