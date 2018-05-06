package com.richstone.mintaka.gempile.dg.mapreduce.impl

import com.richstone.mintaka.gempile.dg.etl.BaseWorker
import com.richstone.mintaka.gempile.dg.etl.impl.HardXdrEtlWorker
import com.richstone.mintaka.gempile.dg.manager.{HardxdrPropManager, MapperManager}
import com.richstone.mintaka.gempile.dg.mapreduce.StatisticsBatchJob
import com.richstone.mintaka.gemstack.common.util.LoggerUtil
import com.richstone.mintaka.gemstack.manager.ApplicationContextManager

/**
  * @Description: ${todo}
  * @author llz
  * @date 2018/5/211:31
  */
class KpiStatisticMapreduceJob extends StatisticsBatchJob
  with HardxdrPropManager with ApplicationContextManager {
  override def excuteJob(): Unit = {
//    val maps = genMapProp(getSysPropertiesFile)
//    info(s"maps is not empty,size is ${maps.keys.size}")

    val hardxdrProps = genHardxdrProp(getSysPropertiesFile)
    if (hardxdrProps != null) if (!hardxdrProps.isEmpty) {
      info(s"hardxdrProps is not empty,size is ${hardxdrProps.size}")
      getBean[BaseWorker]("com.richstone.mintaka.gempile.dg.etl.impl.HardXdrEtlWorker")
        .excuteJob(sc,sqlContext,hardxdrProps)
    }
  }

}
