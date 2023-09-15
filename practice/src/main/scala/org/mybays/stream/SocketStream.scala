/**
 * Created by mybays on 6/17/15.
 */
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.flume._

object SocketStream {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("StreamExample");
    //batch 是以时间为尺度的？
    val ssc=new StreamingContext(conf,Seconds(10));

    //接收一个传统socket流
    val lines=ssc.socketTextStream("localhost",9999);

    //将时间窗内所有的行映射为单词
    val words=lines.flatMap(_.split(" "));

    //计数方法
    var pairs=words.map(word => (word,1))
    val wordCounts=pairs.reduceByKey(_+_);
    wordCounts.print();

    ssc.start();
    ssc.awaitTermination();

  }
}

