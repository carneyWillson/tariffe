import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import constant.myConstant.Time_streaming_secondly
// 小结: 前一分钟, 前一小时这样时间维度的需求, 考虑开窗
// 但是需求中一般不会说是前一分钟, 每一小时...而是会说每一小时

// 如果要求的是1点, 2点这样每个时间段的统计数据, 并不使用开窗
// 我觉得应该是要直接聚合, 用数据(包含时间戳的字段)去计算时间维度
// 因为寄希望于开窗的时间区间和实际时间区间吻合, 很难做到而且没必要
/**########## 需求五 ##########**/
object a5pay_now_province_hourly {
  // (  1,     2,  3,    4,     5,     6,    7,     8)
  // (业务名, 金额, 省, 支付机构, 时长, 时间段, 结果码, 结果信息)
  def run(payStream: DStream[(String, Long, String, String, String, String, String, String)]) = {
    payStream
      .mapPartitions(it => {
        // 省, (金额, 个数)
        it.map(tuple => (tuple._3, (tuple._2, 1L)))
      })
      .reduceByKeyAndWindow((x: Tuple2[Long, Long], y: Tuple2[Long, Long]) => (x._1 + y._1, x._2 + y._2)
        , Minutes(1), Seconds(Time_streaming_secondly))
      .foreachRDD(rdd => {
        rdd.foreachPartition(it => {
          util.mysqlTool.setLast(it)
        })
      })
  }

}
