package util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import scalikejdbc.{AutoSession, ConnectionPool}
import scalikejdbc._
import constant.myConstant._

object mysqlTool {

  // 建立连接, 初始化隐式参数
  Class.forName(mysqlDriver)
  ConnectionPool.singleton("jdbc:mysql://node243:3306/kafka", mysqlUser, mysqlPw)
  implicit val session = AutoSession

  // ?? 需要分别维护每个区间的offset么
  // offset在每个分区间是独立的, 确实需要单独维护
  /**
    * 取出partiton和offset_hi这两列, 存入map中
    * @param topic kafka中对应的主题
    * @return 用于存储kafka指定主题下, 各分区对应的offset
    */
  def getOffsetsMap(topic:String): Map[TopicPartition, Long] = {

    val map = collection.mutable.Map[TopicPartition, Long]()

    sql"""
      select `partition`, rang_hi
      from offset
      where topic=$topic
    """.foreach(resultSet => {
      val partitionIndex = resultSet.get[Int](1)
      val offset = resultSet.get[Int](2)
      map.put(new TopicPartition(topic, partitionIndex), offset)
    })

    map.toMap
  }

  //
  /**
    * kafka0.8直连模式, 和1.0的api, 都需要自己管理和维护offset
    * 该方法用于在操作完成后, 将kafka各分区的角标记录到mysql
    * @param dstream
    */
  def setOffsets(dstream: InputDStream[ConsumerRecord[String, String]]): Unit = {
    dstream.foreachRDD(rdd => {
      // 更新角标
      val offsetArr: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (range <- offsetArr) {
        //	写入mysql数据库
        sql"""
          insert into offset
          (topic, `partition`, rang_lo, rang_hi) values
          (${range.topic}
          ,${range.partition}
          ,${range.fromOffset}
          ,${range.untilOffset})
        """.update().apply()
      }
    })
  }

  /**
    * 业务质量, pay_failed_province_hourly的落地
    * 数据内容是: 省份, (订单量, 成交量)
    * @param it 分区操作rdd, 因此输入是一个迭代器
    */
  def setPayFailed(it: Iterator[(String, (Long, Long))]): Unit = {


    for (elem <- it) {
      // 尝试更新数据
      val flag= sql"""
        update pay_failed_province_hourly
        set payfailed = ${elem._2._1 - elem._2._2}
        where province = ${elem._1}
      """.update().apply()

      // 如果更新失败, 就完整的插入
      if (flag == 0) {
        sql"""
          insert into pay_failed_province_hourly
          (province, payfailed) values
          (${elem._1}
          ,${elem._2._1 - elem._2._2})
        """.update().apply()
      }
    }
  }

  /**
    * 充值订单省份top10, order_top10_province的落地
    * 数据内容是: 省份, (订单量, 成交量)
    * @param it 分区操作rdd, 因此输入是一个迭代器
    */
  def setThird(it: Iterator[(String, (Long, Long))]) = {
    for (elem <- it) {
      val doubleStr = f"${elem._2._2 / elem._2._1.toDouble}%.1f"

      // 尝试更新数据
      val flag= sql"""
        update order_top10_province
        set total_pay = ${elem._2._1}
        , success_rate = ${doubleStr}
        where province = ${elem._1}
      """.update().apply()

      // 如果更新失败, 就完整的插入
      if (flag == 0) {
        sql"""
          insert into order_top10_province
          (province, total_pay, success_rate) values
          (${elem._1}
          ,${elem._2._1}
          ,${doubleStr})
        """.update().apply()
      }
    }

  }

  /**
    * 第四个需求的落地
    * 数据内容是: 时间段, (金额, 次数)
    * @param it 分区操作rdd, 因此输入是一个迭代器
    */
  def setFourth(it: Iterator[(String, (Long, Long))]) = {
    for (elem <- it) {
      // 尝试更新数据
      val flag= sql"""
        update pay_profile_hourly
        set total_pay = ${elem._2._1}
        , total_fee = ${elem._2._2}
        where hour = ${elem._1}
      """.update().apply()

      // 如果更新失败, 就完整的插入
      if (flag == 0) {
        sql"""
          insert into pay_profile_hourly
          (hour, total_pay, total_fee) values
          (${elem._1}
          ,${elem._2._1}
          ,${elem._2._2})
        """.update().apply()
      }
    }

  }

  /**
    * 最后一个需求的落地
    * 数据内容是: 省份, (金额, 个数)
    * @param it 分区操作rdd, 因此输入是一个迭代器
    */
  def setLast(it: Iterator[(String, (Long, Long))]): Unit = {
    for (elem <- it) {
      // 尝试更新数据
      val flag= sql"""
        update pay_now_province_hourly
        set total_pay = ${elem._2._1}
        , total_fee = ${elem._2._2}
        where province = ${elem._1}
      """.update().apply()

      // 如果更新失败, 就完整的插入
      if (flag == 0) {
        sql"""
          insert into pay_now_province_hourly
          (province, total_pay, total_fee) values
          (${elem._1}
          ,${elem._2._1}
          ,${elem._2._2})
        """.update().apply()
      }

    }
  }

























}