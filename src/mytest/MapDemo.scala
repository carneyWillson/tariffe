import scala.collection.mutable.ListBuffer

object MapDemo {
  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[Tuple2[Long, Long]]()
    list.append((1,2), (2, 3))
    def a = list.toMap
  }
}
