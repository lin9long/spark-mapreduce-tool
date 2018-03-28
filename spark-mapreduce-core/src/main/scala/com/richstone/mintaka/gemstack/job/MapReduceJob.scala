package com.richstone.mintaka.gemstack.job

import com.richstone.mintaka.gemstack.conf.SparkConfHolder

abstract trait MapReduceJob extends SparkConfHolder{
  def excuteJob()
}
