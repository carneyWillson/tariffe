import org.apache.spark.sql.Row

import scala.util.parsing.json.JSON

object JsonTest {
  def main(args: Array[String]): Unit = {
    val str = "{\"bussinessRst\":\"0000\",\"channelCode\":\"6900\",\"chargefee\":\"1000\",\"clientIp\":\"117.136.79.101\",\"gateway_id\":\"WXPAY\",\"interFacRst\":\"0000\",\"logOutTime\":\"20170412030030067\",\"orderId\":\"384663607178845909\",\"payPhoneNo\":\"15015541313\",\"phoneno\":\"15015541313\",\"provinceCode\":\"200\",\"rateoperateid\":\"1513\",\"receiveNotifyTime\":\"20170412030030017\",\"requestId\":\"20170412030007090581518228485394\",\"retMsg\":\"接口调用成功\",\"serverIp\":\"10.255.254.10\",\"serverPort\":\"8714\",\"serviceName\":\"payNotifyReq\",\"shouldfee\":\"1000\",\"srcChannel\":\"11\",\"sysId\":\"01\"}"
    def readJson(jsonStr: String) = {
      val infoMap = JSON.parseFull(jsonStr) match {
        case Some(map: Map[String, String]) => map
        case None => Map.empty[String, String]
      }

      infoMap.getOrElse("sysId", "")
      Row()

    }
  }
}
