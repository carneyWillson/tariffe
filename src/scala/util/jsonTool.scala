package util

import java.text.SimpleDateFormat

import org.apache.spark.sql.Row

import scala.util.parsing.json.JSON

object jsonTool {

  /**
    * 用于解析简单的json
    * @param jsonStr 是一个json格式的String
    * @return
    */
  def readJson(jsonStr: String) = {
    JSON.parseFull(jsonStr) match {
      case Some(map: Map[String, String]) => map
      case None => Map.empty[String, String]
    }
  }

  /**
    * 用于将字符串类型的自定义时间格式转换为时间戳
    * @param timestamp 字符串格式的时间戳
    * @param format 时间格式, 默认是"yyyyMMddHHmmssSSS"
    * @return 秒级别的时间戳
    */
  def getTime(timestamp: String
              , format: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS")):Long = {
//    val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    format.parse(timestamp).getTime
  }

}
