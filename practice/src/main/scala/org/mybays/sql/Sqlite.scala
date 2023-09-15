/**
 * Created by mybays on 4/26/15.
 */
import java.sql.DriverManager
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD

object Sqlite {
  def main(args: Array[String]) {
    val sc=new SparkContext("local","sqlite")

    //插入数据
    Class.forName("org.sqlite.JDBC")
    val conn=DriverManager.getConnection("jdbc:sqlite:sample.db");
    val statement =conn.createStatement()
    statement.executeUpdate("drop table if exists test1;\nCREATE TABLE IF NOT EXISTS `test1` (\n\t`id` INTEGER PRIMARY KEY AUTOINCREMENT,\n\t`data` integer,\n\t`time` datetime DEFAULT CURRENT_TIMESTAMP\n);")
    var insert=conn.prepareStatement("insert into test1(data) values(?)")
    (1 to 100).foreach{ i =>
      insert.setInt(1,i*2)
      insert.executeUpdate
    }
    insert.close()


    val rdd=new JdbcRDD(
      sc,
      ()=>{
        Class.forName("org.sqlite.JDBC").newInstance()
        DriverManager.getConnection("jdbc:sqlite:sample.db")
      },
      "select * from test1 where id>=? and id <=?",
      1,100,3,
      r=>r.getString(2)).cache()

    print("Hello\r\n")
    rdd.foreach(print);
    print("Hello\r\n")
    sc.stop()
  }
}
