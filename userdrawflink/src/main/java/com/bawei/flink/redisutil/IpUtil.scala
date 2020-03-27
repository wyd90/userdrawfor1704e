package com.bawei.flink.redisutil

object IpUtil {

  def ip2Long(ip: String): Long = {
    val arr = ip.split("[.]")
    var ipNum = 0L
    arr.foreach(x => {
      ipNum = x.toLong | ipNum << 8L
    })
    ipNum
  }

  def searchIp(javaIpRules: java.util.ArrayList[org.apache.flink.api.java.tuple.Tuple3[java.lang.Long,java.lang.Long,String]], ip: Long): String = {

    import scala.collection.JavaConverters._
    val ipRules = javaIpRules.asScala.map(x => {
      (x.f0.toLong, x.f1.toLong, x.f2)
    }).toArray

    var province = "未知"

    var low = 0;
    var high = ipRules.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if(ip >= ipRules(middle)._1 && ip <= ipRules(middle)._2) {
        return ipRules(middle)._3
      }
      if(ip < ipRules(middle)._1) {
        high = middle - 1
      } else {
        low = middle + 1
      }
    }
    province
  }
}
