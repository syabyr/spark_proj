/**
 * Created by mybays on 5/2/15.
 * 开放数据库处理
 * 将原始的开房数据库过滤处理,生成可用的数据库
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.rdd
import org.apache.spark.rdd._
import org.apache.spark.sql
import org.apache.spark.sql._
case class Customer(name:String,gender:String,ctfId:String,birthday:String,address:String,telephone:String)

object id {
  def main(args:Array[String])
  {
    val idfile="2000W/*.csv"
    val conf = new SparkConf().setAppName("leak id analysis")
    val sc = new SparkContext(conf)
    val sqlContext=new org.apache.spark.sql.SQLContext(sc);
    import sqlContext.implicits._
    //过滤有效的数据,数据有32个分隔符,共有33个字段,另外去除CSV头
    val idall=sc.textFile(idfile,2).filter(line => line.split(",").size == 33).filter( !_.contains("Descriot")).cache();
    //保存正常的数据
    idall.saveAsTextFile("ok")

    //保存不正常的数据
    val idall2=sc.textFile(idfile,2).filter(line => line.split(",").size != 33);
    idall2.saveAsTextFile("notok");

    //按分隔符分开,姓名,性别,身份证号,生日,地址,手机号
    val customer=idall.map(_.split(",")).map(p => Customer(p(0),p(5),p(4),p(6),p(7),p(19))).distinct().toDF();
    customer.registerTempTable("customer")
    //计算总数量
    val count=customer.count();
    println("password count is "+count)

    val test1=sqlContext.sql("select * from customer where telephone = '13606930409' ")
    test1.map(t => "name:" + t(0)).collect().foreach(println);


    sc.stop();
  }
}
