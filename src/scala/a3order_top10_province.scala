import org.apache.spark.streaming.dstream.DStream

/**########## 需求3 ##########**/
object a3order_top10_province {
  // (  1,     2,  3,    4,     5,     6,    7,     8)
  // (业务名, 金额, 省, 支付机构, 时长, 时间段, 结果码, 结果信息)
  def run(orderStream: DStream[(String, Long, String, String, String, String, String, String)]
          , payStream: DStream[(String, Long, String, String, String, String, String, String)]) = {
    val order = orderStream
      .map(tuple => {
        (tuple._3, 1)
      })
      // 想要聚合的话, 就将分区调整为1
      .updateStateByKey((seq: Seq[Int], opt: Option[Long]) => {
        val sum = opt match {
          case Some(l: Long) => l + seq.length
          case None => seq.length
        }
        Option(sum)
      }, 1)
      // 分区取前十
      .mapPartitions(_.toList.sortBy(-_._2).take(10).iterator)

    val pay = payStream
      .map(tuple => {
        (tuple._3, 1)
      })
      .updateStateByKey((seq: Seq[Int], opt: Option[Long]) => {
      val sum = opt match {
        case Some(l: Long) => l + seq.length
        case None => seq.length
      }
      Option(sum)
    })

    // 省份, (订单量, 成交量)
    order.join(pay)
      .foreachRDD(rdd => {
        rdd.foreachPartition(it => {
          util.mysqlTool.setThird(it)
        })
      })


  }

}