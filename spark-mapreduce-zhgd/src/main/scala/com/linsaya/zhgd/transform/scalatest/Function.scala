


/**
  * ${DESCRIPTION}
  *
  * @author llz
  * @create 2018-03-24 21:11
  **/
object Function {

  def main(args: Array[String]): Unit = {
    val list = List("scala", "spark", "hadoop")
    val newlist2 = "test" :: list
    println(list.:+("test"))
    println(list.+:("test"))
//    println(list :: "test")
    println(newlist2.updated(3, "hsda"))
    println(List(1.3, 4.3, 8.2).reduceLeft((sum, x) => sum + x))
    println(variance(Seq(1.3, 4.3)))
    println(variance(Seq(1.3, 4.3, 8.2)))
    if2(true, println(111111111), println(222222))
  }

  def if2[A](condition: Boolean, OnTrue: => A, OnFalse: => A) = {
    if (condition) OnTrue else OnFalse
  }

  def variance(xs: Seq[Double]): Option[Double] = {
    val mid = xs.sum / xs.length
    val variance = xs.flatMap(num => Seq(Math.pow(num - mid, 2))).sum / xs.length
    Some(variance)
    //    xs.flatMap(num => Math.pow(m-num,2)
  }
}
