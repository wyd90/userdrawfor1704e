package com.bawei.ordercaulate

import com.bawei.util.{JedisConnectionPool, JudgeIp}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object CaulateUtil {

  //清洗数据
  def realTimeEtl(lines: RDD[String]) = {
    val arrRdd: RDD[Array[String]] = lines.map(line => {
      val arr = line.split(" ")
      arr
    })

    val fitlered = arrRdd.filter(arr => {
      var flag = false
      try {
        if (arr.length == 6) {
          if (JudgeIp.ipCheck(arr(1))) {
            //判断单价是不是能转成Double类型的
            arr(4).toDouble
            //判断购买数量是不是能转成float类型的
            arr(5).toFloat
            flag = true
          }
        }
      } catch {
        case _: Exception => {
          println("data is err format")
        }
      }
      flag
    })

    val orders = fitlered.map(arr => {

      val ipNum: Long = IpUtil.ip2Long(arr(1))
      val price = arr(4).toDouble
      val amount = arr(5).toFloat
      //用户名，ip，商品类别，单价，购买数量，订单总金额
      (arr(0), ipNum, arr(2), arr(3), price, amount, price * amount)
    })
    orders
  }

  //计算订单交易总额
  def caulateOrderSum(orders: RDD[(String, Long, String, String, Double, Float, Double)]) = {
    val sum: Double = orders.map(_._7).reduce(_+_)
    //driver端打开的conn
    val conn = JedisConnectionPool.getConnection
    conn.incrByFloat("orderSum",sum)
    conn.close()
  }

  def caulateByProvince(orders: RDD[(String, Long, String, String, Double, Float, Double)],rulesRef: Broadcast[Array[(Long, Long, String)]]) = {
    val ipRules = rulesRef.value
    val provinceAndMoney = orders.map(x => {
      val province = IpUtil.searchIp(ipRules, x._2)
      (province, x._7)
    })
    val reduced = provinceAndMoney.reduceByKey(_+_)
    reduced.foreachPartition(it => {
      //在executor中打开的conn
      val conn = JedisConnectionPool.getConnection
      it.foreach(x => {
        conn.incrByFloat(x._1,x._2)
      })
      conn.close()
    })
  }

  def caulateByItem(orders:  RDD[(String, Long, String, String, Double, Float, Double)]) = {
    val reduced: RDD[(String, Double)] = orders.map(x => (x._3,x._7)).reduceByKey(_+_)
    reduced.mapPartitions(it => {
      val conn = JedisConnectionPool.getConnection
      it.foreach(x => {
        conn.incrByFloat(x._1,x._2)
      })
      conn.close()
      it
    })
  }
}
