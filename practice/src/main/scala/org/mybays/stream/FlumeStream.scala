/**
 * Created by mybays on 6/16/15.
 */
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.flume._

object FlumeStream {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("StreamExample");
    //batch 是以时间为尺度的？
    val ssc=new StreamingContext(conf,Seconds(10));

    val flumeStream = FlumeUtils.createStream(ssc,"localhost",9999)

    print("hello1")
    flumeStream.count().map(cnt => "Received " + cnt + " flume events." ).print()
    flumeStream.map(e => "Event:header:" + e.event.get(0).toString
      + "body: " + new String(e.event.getBody.array)).print()


    print("hello2")

    ssc.start();
    ssc.awaitTermination();

  }
}
