object JdbcTest {
  def main(args: Array[String]): Unit = {
    // ScalikeJDBC 1.7 requires SQLInterpolation._ import
    //import scalikejdbc._, SQLInterpolation._
    import scalikejdbc._

    // initialize JDBC driver & connection pool
    Class.forName("com.mysql.jdbc.Driver")
//    ConnectionPool.singleton("jdbc:mysql://node243/kafka", "hive", "hive")
    ConnectionPool.singleton("jdbc:mysql://localhost/cmcc_project", "root", "15935471463")

    // ad-hoc session provider on the REPL
    implicit val session = AutoSession

 /*   val topic = "cmcc_logs"
    val map = collection.mutable.Map[TopicPartition, Long]()

    // 取出partiton和offset_hi这两列, 存入map中
    val maxOffSet = sql"""
      select `partition`, offset_hi
      from offset
      where topic=${topic}
    """.foreach(resultSet => {
      val partitionIndex = resultSet.get[Int](1)
      val offset = resultSet.get[Int](2)
      map.put(new TopicPartition(topic, partitionIndex), offset)
    })*/

    val ele = "'aa', 1, 2, 3"
    val str = ele.toString()
    val topic2 = str.split("'")(1)
    val nums = "\\d+".r.findAllIn(str).toArray
    //	获取连接
    sql"""
          insert into offset
          (topic, `partition`, rang_lo, rang_hi) values
          (${topic2}
          ,${nums(0).toInt}
          ,${nums(1).toInt}
          ,${nums(2).toInt})
        """.update().apply()

    /*
    // table creation, you can run DDL by using #execute as same as JDBC
    sql
create table members (
  id serial not null primary key,
  name varchar(64),
  created_at timestamp not null
)
""".execute.apply()

    // insert initial data
    Seq("Alice", "Bob", "Chris") foreach { name =>
      sql"insert into members (name, created_at) values (${name}, current_timestamp)".update.apply()
    }

    // for now, retrieves all data as Map value
    val entities: List[Map[String, Any]] = sql"select * from members".map(_.toMap).list.apply()

    // defines entity object and extractor
    import org.joda.time._
    case class Member(id: Long, name: Option[String], createdAt: DateTime)
    object Member extends SQLSyntaxSupport[Member] {
      override val tableName = "members"
      def apply(rs: WrappedResultSet) = new Member(
        rs.long("id"), rs.stringOpt("name"), rs.jodaDateTime("created_at"))
    }

    // find all members
    val members: List[Member] = sql"select * from members".map(rs => Member(rs)).list.apply()

    // use paste mode (:paste) on the Scala REPL
    val m = Member.syntax("m")
    val name = "Alice"
    val alice: Option[Member] = withSQL {
      select.from(Member as m).where.eq(m.name, name)
    }.map(rs => Member(rs)).single.apply()
    */
  }

}
