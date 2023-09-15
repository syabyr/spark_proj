/**
 * Created by mybays on 2/19/15.
 */
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object TransForm {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TransForm你好")
    val sc = new SparkContext(conf)
    val distFile=sc.textFile("/Users/mybays/spark/data/data.txt").cache();
    println("Hello World.\r\n")
    distFile.collect.foreach(println);
    val split=distFile.map(line => (line.split('|')(1),line));
    val splita=split.collect();

    //group
    val test1=split.groupByKey();
    val test11=test1.collect();

    //在聚合后的分类里做处理
    val test2=test1.map(line=>(line._1,{
      var str="";
      val size=line._2.size;
      var i=0;
      line._2.foreach((arg:String) => str+=arg);
      str;
      /*
      while (i<size)
      {
        str+=line._2(i);
        i+=1;
      }
      */
    }));

    val test22=test2.collect();

    sc.stop()
  }
}
