package com.linsaya.zhgd.transform.scalatest

/**
  * ${DESCRIPTION}
  *
  * @author llz
  * @create 2018-03-23 23:35
  **/
object ScalaFunctionTest  {
  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 3, 4)
    val list2 = List(1.1, 2.2, 3.3, 4.4)
    println(List.sum(list))
    println(List.product(list2))
    List(1, 2, 3, 4) match {
      case Cons(a, _) => println(a)
      case Cons(x1, x2) => println(x2)
    }

    val x = List(1, 2, 3, 4, 5) match {
      case Cons(x, Cons(2, Cons(4, _))) => println(x)
      case Nil => 42
      //      case Cons(x, Cons(y, Cons(3, Cons(4, _)))) => println(x + y)
      case Cons(h, t) => h + List.sum(t)
      case _ => 100
    }
    println("1--------->" + List.tail(List(1, 2, 3, 4)))
    println("2--------->" + List.setHead(List(1, 2, 3, 4), 100))
    println("3--------->" + List.drop(List(1, 2, 3, 4, 6, 7, 77, 7, 666), 100))
    println("4--------->" + List.dropWhile(List(1, 2, 3, 4, 3, 7, 77, 7, 666), (x: Int) => x == 3))
    println("4--------->" + List.dropWhileCurr(List(1, 2, 3, 4, 3, 6, 77, 7, 666))(x => x < 7))
    println("5--------->" + List.sum2(List(1, 2, 3, 4, 3, 6, 77, 7, 666)))
    println("6--------->" + List.product2(List(1.1, 2.2, 3.3, 4.4)))
    println("6--------->" + List.foldRight(List(1,2,3,4,5),Nil:List[Int])(Cons(_,_)))
  }
}
