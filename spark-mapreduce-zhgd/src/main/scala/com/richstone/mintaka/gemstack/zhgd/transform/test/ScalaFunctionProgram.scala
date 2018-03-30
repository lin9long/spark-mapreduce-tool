package com.richstone.mintaka.gemstack.zhgd.transform.test

/**
  * @Description: ${todo}
  * @author llz
  * @date 2018/3/2116:00
  */
object ScalaFunctionProgram {

  def main(args: Array[String]): Unit = {

    println("900%1000---->" + 900 % 1000)

    def msgFormat(msg: String, n: Int, f: Int => Int): Unit = {
      val newmsg = msg.format(f(n), f(n))
      println(newmsg)
    }

    val msg = "num is %d and %d"
    msgFormat(msg, 3, factorial)


    def factorial(int: Int): Int = {
      def go(n: Int, acc: Int): Int = {
        if (n <= 0) acc
        else go(n - 1, n * acc)
      }

      go(int, 1)
    }

    println(factorial(3))

    def fib(n: Int): Int = {
      def go(level: Int): Int = {
        if (level == 0) 0
        else if (level == 1) 1
        else {
          go(level - 1) + go(level - 2)
        }
      }

      go(n)
    }

    println(fib(6))


    def findTheFirst[A](array: Array[A], f: A => Boolean): Int = {
      def loop(n: Int): Int = {
        if (n > array.length - 1) -1
        else if (f(array(n))) n
        else loop(n + 1)
      }

      loop(0)
    }

    def find(str: String): Boolean = {
      val arr = Array("1", "2", "3", "4")
      arr.contains(str)
    }

    val arr = Array("5", "6", "4")
    println(findTheFirst[String](arr, (x: String) => x == "100"))

    println(s"Length is ${List.length(List(1, 2, 3, 4, 1, 2, 3, 213, 12))}")

    println(s"List.foldLeft is ${List.foldLeft(List(1, 2, 3, 4, 1, 2, 3, 213, 12), 0)((x, y) => x + y)}")

    println(s"List.sum3 is ${List.sum3(List(1, 2, 3, 4, 1, 2, 3, 213, 12))}")

    println(s"List.append is ${List.append(List(1, 2, 3, 4, 1, 2), List(3, 213, 12))}")

    println(List.map2(List(1, 2, 3, 4, 1, 2))(x => x + 1))
    println(List.mapToString(List(100.1, 123, "你好", "eeeee", 1, 2))(x => x.toString))
    println(List.filter(List(100.1, 123, 2222, 198, 331, 222))(x => x < 400))
    println(List.reverse(List(100.1, 123, 2222, 198, 331, 222)))
  }

}
