import java.text.SimpleDateFormat

import constant.myConstant._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object RddDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("demo")
      .setMaster("local[*]")

    val ss = new SparkContext(conf)


    val source: RDD[String] = ss.textFile("C:\\Users\\WangJiaLi\\Desktop\\充值平台实时统计分析\\cmcc.json")

    val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    val allInfoStream = source.map(line => {
      val infoMap = util.jsonTool.readJson(line)

      val startTime = infoMap.getOrElse(Col_startReqTime, "19700101080000000")
      // 结束时间
      val endTime = infoMap.getOrElse(Col_endReqTime, "19700101080000000")
      val timerange = util.jsonTool.getTime(endTime, format) - util.jsonTool.getTime(startTime, format)
      (
        // 服务名
        infoMap.getOrElse(Col_serviceName, ""),
        // 结果
        infoMap.getOrElse(Col_retMsg, ""),
        // 省份
        infoMap.getOrElse(Col_provinceCode, ""),
        // 支付机构
        infoMap.getOrElse(Col_gateway_id, ""),
        timerange.toString
      )
    }).filter(tuple => {
      println(tuple)
      var flag = false
      if (tuple._1 == "payNotifyReq") flag = true
      if (tuple._1 == "sendRechargeReq" && tuple._2 == "成功") flag = true
      flag
    }).cache()


    allInfoStream.map(tuple => {
      (tuple._1, 1)
    }).reduceByKey(_ + _)
      .foreach(println)


  }
}
