/**
 * Created by mybays on 2/15/15.
 * CSDN密码处理
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.rdd
import org.apache.spark.rdd._

object csdnpasswd {
  def main(args:Array[String])
  {
    //val passwdfile="workspace/test.txt"
    val passwdfile="wwwcsdnnet.sql"
    val conf = new SparkConf().setAppName("csdn password analysis")
    val sc = new SparkContext(conf)
    val passwd=sc.textFile(passwdfile,2).cache();
    //按分隔符分开
    val passwddata=passwd.map(line=>line.split(" # ")).map(line => {(line(0),line(1),line(2))});
    //将所有的元素打印下来
    //passwddata.collect.foreach(println);
    //取出特定的元素
    //val mybays=passwddata.filter(line=>line._1.contains("mybays")).cache();
    //mybays.collect.foreach(println);
    //计算总数量
    val count=passwddata.count();
    println("password count is "+count)

    //找到用的最多的密码(前10)

    //将密码设置为主键
    val prefrequency=passwddata.map(line=>(line._2,line))
    //使用密码主键来分组
    val frequency=prefrequency.groupByKey;
    //统计密码的使用频率
    val frequency2=frequency.map(line=>(line._1,line._2.size));
    //找到密码使用频率大于1000的
    val frequency3=frequency2.filter(line=>(line._2>1000));
    //将使用频率和密码位置交换,然后升序排序
    val frequency4=frequency3.map(line=>(line._2,line._1)).sortByKey();
    frequency4.collect.foreach(println);

    //

    sc.stop();
  }
}
