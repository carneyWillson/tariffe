import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import constant.myConstant._
import org.apache.spark.api.java.function
import redis.clients.jedis.Jedis

object DoMain {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("cmcc")
      // 指定序列化格式以提高效率
      .set("spark.serilizer", "org.apache.spark.serializer.KryoSerializer")


    val streamingContext = new StreamingContext(conf, Seconds(Time_streaming_secondly))
    streamingContext.checkpoint(Path_chickpoint)
    // 将省份信息广播出去
    // 需要注意的是不能new, 因为一个jvm只能运行一个sc
    val sc = streamingContext.sparkContext
    val cityMap = sc.textFile(Path_city_text)
      .map(line => {
        val strArr = line.split(" ")
        (strArr(0), strArr(1))
      })
      .collect()
      .toMap
    sc.broadcast(cityMap)

    val topic = Kafka_topic
    // 根据topic, 从mysql中得到offset集
    val offsetsMap = util.mysqlTool.getOffsetsMap(topic)

   val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> Kafka_bootstrap_servers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> Kafka_group_id,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      // 连接
      streamingContext,
      // 选择模式
      PreferConsistent,
      // topic, 配置参数, map用于记录各分区的角标
      Subscribe[String, String](Array(topic), kafkaParams, offsetsMap)
    )


    // 提前建立format对象, 防止重复new
    val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    val allInfoStream = stream.map(consumer => {
      // 将json解析为Map
      val jsonStr = consumer.value()
      val infoMap: Map[String, String] = util.jsonTool.readJson(jsonStr)

      // 求时间范围
      val startTime = infoMap.getOrElse(Col_startReqTime, Time_initial_value)
      val endTime = infoMap.getOrElse(Col_endReqTime, Time_initial_value)
      val timerange = util.jsonTool.getTime(endTime, format) - util.jsonTool.getTime(startTime, format)

      val provinceCode = infoMap.getOrElse(Col_provinceCode, "")
      // 返回一个元组, 因为元素少, 而且这里的row也不方便配合schema
      (
        // 服务名
        infoMap.getOrElse(Col_serviceName, ""),
        // 金额(单位是分)
        infoMap.getOrElse(Col_chargefee, "0").toLong,
        // 省份
        cityMap.getOrElse(provinceCode, "未知省份"),
        // 支付机构
        infoMap.getOrElse(Col_gateway_id, ""),
        // 时长
        timerange.toString,
        // 时间段
        startTime.substring(8, 10),
        // 业务结果
        infoMap.getOrElse(Col_bussinessRst, ""),
        // 结果
        infoMap.getOrElse(Col_retMsg, "")
      )
    }).cache()

    // 第一个需求
    a1business_profile.run(allInfoStream)

    // 进行分表
    val orderStream = allInfoStream
      .filter(tuple => {
        if (tuple._1 == "payNotifyReq" && tuple._7 == "0000") true else false
      }).cache()

    var payStream = allInfoStream
      .filter(tuple => {
        if (tuple._1 == "sendRechargeReq" && tuple._8 == "成功") true else false
      }).cache()

    // 第二个需求
    a2pay_failed_province_hourly.run(orderStream, payStream)
    // 第三个需求
    a3order_top10_province.run(orderStream, payStream)
    // 第四个需求
    a4pay_profile_hourly.run(payStream)
    // 最后一个需求
    a5pay_now_province_hourly.run(payStream)


    // 将角标存入mysql
    util.mysqlTool.setOffsets(stream)
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
