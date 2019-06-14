import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

// 没有发生交互的数据, 不需要join
// 一张大表就可以搞定

// 考虑使用窗口, 只处理最新一小时的数据
/**########## 需求1 ##########**/
object a1business_profile {
  def run(allInfoStream: DStream[(String, Long, String, String, String, String, String, String)]) = {
    // (  1,     2,  3,    4,     5,     6,    7,     8)
    // (业务名, 金额, 省, 支付机构, 时长, 时间段, 结果码, 结果信息)
    allInfoStream
      .filter(tuple => {
        if (tuple._1 == "payNotifyReq" && tuple._7 == "0000"
          || tuple._1 == "sendRechargeReq" && tuple._8 == "成功") true else false
      })
      .map(tuple => {
        // key是业务名, 值是金额和次数
        (tuple._1, (tuple._2, 1L))
      })
      .updateStateByKey((seq, sum: Option[Tuple2[Long, Long]]) => {
        val ot = sum match {
          case Some(x) => x
          case None => (0L, 0L)
        }
        val totalFee = seq.foldLeft(0L)(_ + _._1)
        Option((ot._1 + totalFee, ot._2 + seq.length))
      })
      .foreachRDD(rdd => {
        rdd.foreachPartition(it => {
          val jedis = new Jedis("node243", 6379)
          for (tuple <- it) {
            // 充值订单量, 充值金额, 充值成功数
            if (tuple._1 == "payNotifyReq") {
              jedis.set("充值订单量", tuple._2._2.toString)
              jedis.set("充值金额",tuple._2._1.toString)
            } else if (tuple._1 == "sendRechargeReq") {
              jedis.set("充值成功数", tuple._2._2.toString)
            }
          }
        })
      })
  }
}
