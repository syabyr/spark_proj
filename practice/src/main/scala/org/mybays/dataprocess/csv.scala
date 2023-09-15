/**
 * Created by mybays on 2/19/15.
 * 伪CSV文件读写测试
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._

object csv {
  def main(args: Array[String]) {
    val logFile = "test.csv" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    //按分隔符展开
    val aaa=logData.map(line => line.split(","))
    //val bbb=aaa.map(line => line(0));
    //bbb.collect.foreach(println);

    val ccc=aaa.map(line => {(line(0),(line(1),line(2),line(3),line(4),line(5)))})
    //第一步输出
    ccc.collect.foreach(println)

    //out,csv文件格式不能有问题,否则会出现错误
    val outData=sc.textFile("test.csv",2).cache()
    //
    val outAAA=outData.map(line=>line.split(','))
    val outCCC=outAAA.map(line => {(line(0),line(1))})
    outCCC.collect.foreach(println)
    //split line(1)
    val outDDD=outAAA.map(line => {(line(0),line(1))})
    outDDD.collect.foreach(println)
    val outEEE=outAAA.map(line => {line(1)})
    outEEE.collect.foreach(println)

    val outFFF=outEEE.map(line => line.split('|'))
    //异常处理
    val outGGG=outFFF.filter(line=>line.length == 2).map(line => {(line(0),line(1))})
    outGGG.collect.foreach(println)

    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    sc.stop()
  }
}
