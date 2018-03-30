package com.richstone.mintaka.gemstack.zhgd.transform.test

/**
  * @Description: ${todo}
  * @author llz
  * @date 2018/3/2414:49
  */

sealed trait List[+A]

case object Nil extends List[Nothing]

case class Cons[+A](head: A, tail: List[A]) extends List[A]

case class Constail[+A](head: List[A], tail: A) extends List[A]

object List {


  def foldRight[A, B](as: List[A], z: B)(f: (A, B) => B): B = {
    as match {
      case Nil => z
      case Cons(x, ds) => f(x, foldRight(ds, z)(f))
    }
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

  def sum2(as: List[Int]): Int = {
    foldRight(as, 0)((x, y) => x + y)
  }

  def length[A](as: List[A]): Int = {
    foldRight(as, 0)((x, y) => y + 1)
  }

  def apply[A](any: A*): List[A] = {
    if (any.isEmpty) Nil
    else Cons(any.head, apply(any.tail: _*))
  }


}
