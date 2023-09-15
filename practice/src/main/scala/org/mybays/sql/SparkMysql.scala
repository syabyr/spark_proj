/**
 * Created by mybays on 4/25/15.
 */
import java.sql.DriverManager
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD

object SparkMysql {
  def main(args: Array[String]) {
    val sc=new SparkContext("local","mysql")

    //插入数据
    val conn=DriverManager.getConnection("jdbc:mysql://202.1.1.190:3306/mybays","mybays","mybays");
    var insert=conn.prepareStatement("insert into test(data) values(?)")
    (1 to 100).foreach{ i =>
      insert.setInt(1,i*2)
      insert.setInt(1,i*2)
      insert.executeUpdate
    }
    insert.close()


    val rdd=new JdbcRDD(
      sc,
      ()=>{
        Class.forName("com.mysql.jdbc.Driver").newInstance()
        DriverManager.getConnection("jdbc:mysql://202.1.1.190:3306/mybays","mybays","mybays")
      },
      "select * from test where id>=? and id <=?",
      1,100,3,
      r=>r.getString(2)).cache()

    print("Hello\r\n")
    rdd.foreach(print);
    print("Hello\r\n")
    sc.stop()
  }
}
