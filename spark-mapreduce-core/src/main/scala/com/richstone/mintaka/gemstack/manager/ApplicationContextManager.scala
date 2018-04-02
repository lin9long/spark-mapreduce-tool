package com.richstone.mintaka.gemstack.manager

/**
  * ${DESCRIPTION}
  *
  * @author llz
  * @create 2018-04-02 23:36
  **/
trait ApplicationContextManager {

  /**
   * @Description: 根据类名获取对应的对象
   * @param: [clazzName]
   * @return: A
   * @author: llz
   * @Date: 2018/4/2
   */
  def getBean[A](clazzName: String): A = {
    val clazz = Class.forName(clazzName)
    clazz.newInstance().asInstanceOf[A]
  }
}
