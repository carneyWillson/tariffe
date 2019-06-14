import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object chickSource {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("demo")
      .setMaster("local[*]")


    val ssc: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val frame: DataFrame = ssc.read.json("C:\\Users\\WangJiaLi\\Desktop\\充值平台实时统计分析\\cmcc.json")
    frame.schema.foreach(println)
    frame.select("bussinessRst").distinct().show()
    frame.where("serviceName = 'sendRechargeReq'").select("bussinessRst").distinct().show()
    // 看一下支付信息
    frame.select("retMsg").distinct().show()
    frame.select("serviceName").distinct().show()


  }
}
