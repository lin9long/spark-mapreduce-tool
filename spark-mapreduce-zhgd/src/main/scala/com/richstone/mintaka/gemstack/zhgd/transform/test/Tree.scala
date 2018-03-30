package com.richstone.mintaka.gemstack.zhgd.transform.test

/**
  * @Description: ${todo}
  * @author llz
  * @date 2018/3/2417:48
  */

sealed trait Tree[+A]

case class Leaf[A](value: A) extends Tree[A]

case class Branch[A](left: Tree[A], right: Tree[A]) extends Tree[A]

object Tree {

  def size[A](tree: Tree[A]): Int = {
    tree match {
      case Leaf(_) => 1
      case Branch(l, r) => size(l) + size(r) + 1
    }
  }

  def maximum[Int](tree: Tree[Int]): Int = {
    var temp = 0
    tree match {
      case Leaf(x) => x
//      case Branch(l, r) => maximum(l) max maximum(r)
    }
  }
}
