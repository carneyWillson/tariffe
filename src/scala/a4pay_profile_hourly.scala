import org.apache.spark.streaming.dstream.DStream

/**########## 需求4 ##########**/
object a4pay_profile_hourly {
  // (  1,     2,  3,    4,     5,     6,    7,     8)
  // (业务名, 金额, 省, 支付机构, 时长, 时间段, 结果码, 结果信息)
  def run(payStream: DStream[(String, Long, String, String, String, String, String, String)]) = {
    payStream
      .mapPartitions(it => {
        // 时间段, (金额, 个数)
        it.map(tuple => (tuple._6, (tuple._2, 1)))
      })
      .updateStateByKey((seq, sum: Option[Tuple2[Long, Long]]) => {
        val opt: Tuple2[Long, Long] = sum match {
          case Some(x: Tuple2[Long, Long]) => (seq.map(_._1).sum + x._1, x._2 + seq.length)
          case None => (0, 0)
        }
        Option(opt)
      })
      .foreachRDD(rdd => {
        rdd.foreachPartition(it => {
          util.mysqlTool.setFourth(it)
        })
      })
  }

}
