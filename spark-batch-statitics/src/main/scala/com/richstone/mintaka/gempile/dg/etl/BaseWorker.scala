package com.richstone.mintaka.gempile.dg.etl

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * @Description: ${todo}
  * @author llz
  * @date 2018/5/216:59
  */
abstract trait BaseWorker extends Serializable{

  def excuteJob[A](sc: SparkContext,sqlContext:SQLContext, props: IndexedSeq[A])

}
