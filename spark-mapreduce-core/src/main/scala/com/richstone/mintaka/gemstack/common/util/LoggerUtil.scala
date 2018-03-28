package com.richstone.mintaka.gemstack.common.util

import java.io.{FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import org.slf4j.LoggerFactory

/**
  * @Description:log工具类
  * @param:
  * @return:
  * @author: llz
  * @Date: 2018/3/28
  */
trait LoggerUtil {

  private[this] val logger = LoggerFactory.getLogger(getClass().getName())

  val logPath =s"./log/${getClass.getName}.log"

  def debug(message: => String): Unit =
    logger.debug(message)

  def debug(message: => String, ex: Throwable): Unit =
    logger.debug(message, ex)

  def debugValue[T](valueName: String, value: => T): T = {
    val result: T = value
    debug(valueName + " == " + result.toString)
    result
  }

  def info(message: => String): Unit =
    logger.info(message)

  def info(message: => String, ex: Throwable): Unit =
    logger.info(message, ex)

  def warn(message: => String): Unit =
    logger.warn(message)

  def warn(message: => String, ex: Throwable): Unit =
    logger.warn(message, ex)

  def error(ex: Throwable): Unit =
    logger.error(ex.toString, ex)

  def error(message: => String): Unit =
    logger.error(message)

  def error(message: => String, ex: Throwable): Unit =
    logger.error(message, ex)

  def log(msg: String, level: String = "DEBUG"): Unit = {
    val writer = new PrintWriter(new FileWriter(logPath, true))
    val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val datetime = fmt.format(new Date())
    writer.println(s"$datetime $level: $msg")
    writer.close()
  }

}
