import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by mybays on 5/6/15.
 */
object testjson {
  def main(args:Array[String]): Unit =
  {
    val conf = new SparkConf().setAppName("testjson")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val df = sqlContext.jsonFile("people.json")

    // Displays the content of the DataFrame to stdout
    df.show()
    df.printSchema();
    df.select("name").show();
    df.select(df("name"),df("age")+1).show();
    df.filter(df("age")>21).show();
    df.groupBy("age").count().show();
    //保存数据
    //df.select("name", "age").save("namesAndAges.parquet")
    //注册成一个数据表
    df.registerTempTable("people");
    val test = sqlContext.sql("SELECT * FROM people");
    print("helloworld\r\n")
    test.map(t=>"Name: "+t(0)).collect().foreach(println);
  }
}
