package com.bawei.ordercaulate

import com.bawei.util.{JedisConnectionPool, JudgeIp}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, Milliseconds, Seconds, StreamingContext}

/**
 * A 202.106.196.115 手机 iPhone8 8000 1
 * B 202.106.0.20 服装 布莱奥尼西服 199 2
 * C 202.102.152.3 家具 婴儿床 2000 1
 * D 202.96.96.68 家电 电饭锅 1000 1
 * F 202.98.0.68 化妆品 迪奥香水 200 5
 * H 202.96.75.68 食品 奶粉 600 10
 * J 202.97.229.133 图书 Hadoop编程指南 90
 * A 202.106.196.115 手机 手机壳 200
 * B 202.106.0.20 手机 iPhone8 8000
 * C 202.102.152.3 家具 婴儿车 2000
 * D 202.96.96.68 家具 婴儿车 1000
 * F 202.98.0.68 化妆品 迪奥香水 200
 * H 202.96.75.68 食品 婴儿床 600
 * J 202.97.229.133 图书 spark实战 80
 */
//kafka队列=userorder，groupid=userorderconsumer
object OrderCaulate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ordercaulate")
    val ssc = new StreamingContext(conf, Seconds(5))

    val ipText = ssc.sparkContext.textFile("C:\\yarnData\\ip\\rule")
    val ruleRdd: RDD[(Long, Long, String)] = ipText.map(line => {
      val arr = line.split("[|]")
      (arr(2).toLong, arr(3).toLong, arr(6))
    })

    val rules: Array[(Long, Long, String)] = ruleRdd.collect()

    val rulesRef: Broadcast[Array[(Long, Long, String)]] = ssc.sparkContext.broadcast(rules)

    val Array(brokers, groupId, topics) = args
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",  //latest
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean))

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    messages.foreachRDD(kafkaRdd => {
        if(!kafkaRdd.isEmpty()) {
          //取出这批次的kafka偏移量
          val offsetRanges = kafkaRdd.asInstanceOf[HasOffsetRanges].offsetRanges

          //开始自己的业务逻辑
          val lines: RDD[String] = kafkaRdd.map(x => {x.value()})

          //实时etl，做数据清洗
          val orders: RDD[(String, Long, String, String, Double, Float, Double)] = CaulateUtil.realTimeEtl(lines)

          //计算业务指标
          if(!orders.isEmpty()) {
            //计算实时交易总金额
            CaulateUtil.caulateOrderSum(orders)
            //按地区计算交易额
            CaulateUtil.caulateByProvince(orders,rulesRef)
            //按商品类别统计
          }

          //---------------------------

          //提交kafka偏移量
          messages.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
