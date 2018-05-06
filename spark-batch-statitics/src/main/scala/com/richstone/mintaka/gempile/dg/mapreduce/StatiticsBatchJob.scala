package com.richstone.mintaka.gempile.dg.mapreduce

import com.richstone.mintaka.gemstack.conf.SparkConfHolder


/**
  * @Description: ${todo}
  * @author llz
  * @date 2018/5/211:28
  */
abstract trait StatisticsBatchJob extends SparkConfHolder with Serializable{
 def excuteJob()
}
