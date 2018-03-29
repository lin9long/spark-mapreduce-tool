package com.linsaya.zhgd.transform.scalatest

/**
  * 自定义list方法
  *
  * @author llz
  * @create 2018-03-23 23:24
  **/

sealed trait List[+A]

case object Nil extends List[Nothing]

case class Cons[A](head: A, tail: List[A]) extends List[A]

object List {

  def sum(ints: List[Int]): Int = ints match {
    case Nil => 0
    case Cons(x, xs) => x + sum(xs)
  }

  def product(ds: List[Double]): Double = ds match {
    case Nil => -1.0
    case Cons(0.0, _) => 0.0
    case Cons(x, xs) => x * product(xs)
  }

  def setHead[A](ds: List[A], head: A) = ds match {
    case Nil => Nil
    case Cons(x, Nil) => Cons(head, Nil)
    case Cons(x, xs) => Cons(head, xs)
  }

  def drop[A](ds: List[A], n: Int): List[A] = ds match {
    case Cons(x, xs) => if (n > 0) drop(tail(ds), n - 1) else ds
    case Cons(x, Nil) => Nil
    case Nil => Nil
  }

  def dropWhile[A](ds: List[A], f: A => Boolean): List[A] = ds match {
    case Cons(x, xs) => if (f(x)) dropWhile(tail(ds), f) else Cons(x, dropWhile(xs, f))
    case Cons(x, Nil) => if (f(x)) tail(ds) else ds
    case Nil => Nil
  }

  def dropWhileCurr[A](ds: List[A])(f: A => Boolean): List[A] = ds match {
    case Cons(x, xs) if f(x) => dropWhileCurr(xs)(f)
    case _ => ds
  }

  def tail[A](ds: List[A]): List[A] = {
    ds match {
      case Cons(x, xs) => xs
      case Cons(x, Nil) => Nil
      case Nil => Nil
    }
  }

  /**
    * @Description: 折叠操作
    * @param: [as, z, f]
    * @return: ((A, B) => B) => B
    * @author: llz
    * @Date: 2018/3/24
    */
  def foldRight[A, B](as: List[A], z: B)(f: (A, B) => B): B = {
    as match {
      case Nil => z
      case Cons(x, xs) => f(x, foldRight(xs, z)(f))
    }
  }

  def sum2(as: List[Int]) = {
    foldRight(as, 0)((x, y) => x + y)
  }

  def product2(as: List[Double]) = {
    foldRight(as, 1.0)((x, y) => x * y)
  }


  def foldLeft[A, B](as: List[A], z: B)(f: (B, A) => B): B = {
    as match {
      case Nil => z
      case Cons(x, xs) => foldLeft(xs, f(z, x))(f)
    }
  }

  def append[A](a1: List[A], a2: List[A]): List[A] = {
    a2 match {
      case Nil => a1
      case Cons(x, ds) => Cons(x, append(a1, ds))
    }
  }

  def map2(as: List[Int])(f: Int => Int): List[Int] = {
    as match {
      case Nil => Nil
      case Cons(x, ds) => Cons(f(x), map2(ds)(f))
    }
  }

  def mapToString[A](as: List[A])(f: A => String): List[String] = {
    as match {
      case Nil => Nil
      case Cons(x, ds) => Cons(f(x), mapToString(ds)(f))
    }
  }

  def map[A, B](as: List[A])(f: A => B): List[B] = {
    as match {
      case Nil => Nil
      case Cons(x, ds) => Cons(f(x), map(ds)(f))
    }
  }

  def head[A](as: List[A]): A = {
    as match {case Cons(x, ds) => x}
  }

  def reverse[A](as: List[A]): List[A] = {
    as match {
      case Nil => Nil
      case Cons(x, ds) => Cons(head(ds), append(ds, List(x)))
      //      case Constail(ls, x) => reverse(Cons(x, ls))
      case _ => as
    }
  }


  def filter[A](as: List[A])(f: A => Boolean): List[A] = {

    //    foldLeft(as,f)
    as match {
      case Cons(x, ds) if (f(x)) => filter(ds)(f)
      case _ => as
    }
  }


  def sum3(as: List[Int]): Int = {
    foldLeft(as, 0)(_ + _)
  }


  def length[A](as: List[A]): Int = {
    foldRight(as, 0)((x, y) => y + 1)
  }


  def apply[A](as: A*): List[A] =
    if (as.isEmpty) Nil
    else Cons(as.head, apply(as.tail: _*))

}
