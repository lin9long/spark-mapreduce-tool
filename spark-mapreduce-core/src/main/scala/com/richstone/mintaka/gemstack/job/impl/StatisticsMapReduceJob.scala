package com.richstone.mintaka.gemstack.job.impl

import com.richstone.mintaka.gemstack.job.MapReduceJob

/**
  * @Description: 数据统计逻辑
  * @param:
  * @return:
  * @author: llz
  * @Date: 2018/3/28
  */
@SerialVersionUID(9146747181746894650L)
class StatisticsMapReduceJob extends MapReduceJob {
  override def excuteJob(): Unit = {
    println("excuteJob------->StatisticsMapReduceJob")
  }
}
