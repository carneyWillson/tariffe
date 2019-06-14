package constant

object myConstant {
  final val Col_bussinessRst = "bussinessRst"
  final val Col_channelCode = "channelCode"
  final val Col_chargefee = "chargefee"
  final val Col_clientIp = "clientIp"
  final val Col_endReqTime = "endReqTime"
  final val Col_gateway_id = "gateway_id"
  final val Col_idType = "idType"
  final val Col_interFacRst = "interFacRst"
  final val Col_logOutTime = "logOutTime"
  final val Col_orderId = "orderId"
  final val Col_payPhoneNo = "payPhoneNo"
  final val Col_phoneno = "phoneno"
  final val Col_prodCnt = "prodCnt"
  final val Col_provinceCode = "provinceCode"
  final val Col_rateoperateid = "rateoperateid"
  final val Col_receiveNotifyTime = "receiveNotifyTime"
  final val Col_requestId = "requestId"
  final val Col_resultTime = "resultTime"
  final val Col_retMsg = "retMsg"
  final val Col_serverIp = "serverIp"
  final val Col_serverPort = "serverPort"
  final val Col_serviceName = "serviceName"
  final val Col_shouldfee = "shouldfee"
  final val Col_srcChannel = "srcChannel"
  final val Col_startReqTime = "startReqTime"
  final val Col_sysId = "sysId"

  // 用于连接jdbc
  final val mysqlDriver = "com.mysql.jdbc.Driver"
  final val mysqlUrl = "jdbc:mysql://node243:3306/kafka"
  final val mysqlUser = "hive"
  final val mysqlPw = "hive"

  // 文件位置
  final val Path_city_text = "C://myDemo/city.txt"
  final val Path_chickpoint = "c://mydemo/ckpoint"

  // kafka相关属性
  final val Kafka_topic = "cmcc_logs"
  final val Kafka_group_id = "wjl_group"
  final val Kafka_bootstrap_servers = "node243:9092,node244:9092"

  // 其它
  final val Time_streaming_secondly = 6
  final val Time_initial_value = "19700101080000000"


}
