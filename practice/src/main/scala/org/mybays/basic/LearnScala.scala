package org.mybays.basic

/**
 * Created by mybays on 2/19/15.
 * Learn Scala
 */
object LearnScala {

  class ChecksumAccumulator{
    private var sum=0;

    def add(b:Byte):Unit =sum+=b;
    def checksum():Int = ~(sum & 0xff)+1;

  }

  def main (args:Array[String])= {
    println("Hello World!\r\n")
    val acc=new ChecksumAccumulator;
    val 你好="你好hello\r\n";
    println(你好);
  }
}
