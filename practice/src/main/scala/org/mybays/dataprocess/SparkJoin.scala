/**
 * Created by mybays on 2/19/15.
 * Data:http://files.grouplens.org/datasets/movielens/ml-1m.zip
 */
import org.apache.spark._
import SparkContext._

object SparkJoin {
  def main(args:Array[String]){
    val conf = new SparkConf().setAppName("Spark Join")
    val sc=new SparkContext(conf)
    val textFile=sc.textFile("ml-1m/ratings.dat")

    //rating=(电影ID,评分)
    val rating=textFile.map(line=>{
      val fileds=line.split("::")
      (fileds(1).toInt,fileds(2).toDouble)
    })

    //movieScores=(电影ID,平均分)
    val movieScores=rating
      .groupByKey()
      .map(data=>{
      val avg=data._2.sum/data._2.size
      (data._1,avg)
    })

    //movieskey=(电影ID,(电影ID,电影名称))
    val movies=sc.textFile("ml-1m/movies.dat")
    val movieskey=movies.map(line=>{
      val fileds=line.split("::")
      (fileds(0).toInt,fileds(1))
    }).keyBy(tup => tup._1)

    //result=(电影ID,(电影ID,电影平均分))
    val result=movieScores.keyBy(tup=>tup._1);

    // result1=(电影ID,((电影ID,电影平均分),(电影ID,电影名称)))
    val result1=result.join(movieskey);

    //result2=过滤后的,(电影ID,电影平均分,电影名称)
    val result2=result1.filter(f=>f._2._1._2>4.0).map(f=>(f._1,f._2._1._2,f._2._2._2))
    result2.collect.foreach(println)
    // result1.saveAsTextFile("ml-1m/a.txt")
    println("exit......")
    sc.stop()
  }
}
