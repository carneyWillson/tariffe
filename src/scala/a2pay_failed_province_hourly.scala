import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream

/**########## 需求2 ##########**/
object a2pay_failed_province_hourly {
  // (  1,     2,  3,    4,     5,     6,    7,     8)
  // (业务名, 金额, 省, 支付机构, 时长, 时间段, 结果码, 结果信息)
  def run(orderStream: DStream[(String, Long, String, String, String, String, String, String)]
          , payStream: DStream[(String, Long, String, String, String, String, String, String)]) = {
    // 所有订单
    val order = orderStream
      .map(tuple => {
        (tuple._3, 1L)
      })
      .reduceByKeyAndWindow((x:Long, y:Long) => x + y, Minutes(60), Seconds(6))

    // 支付成功
    val pay = payStream
      .map(tuple => {
        (tuple._3, 1L)
      })
      .reduceByKeyAndWindow((x:Long, y:Long) => x + y, Minutes(60), Seconds(6))


    // 省份, (订单量, 成交量)
    order.join(pay)
      .foreachRDD(rdd => {
        rdd.foreachPartition(it => {
          util.mysqlTool.setPayFailed(it)
        })
      })


  }

}