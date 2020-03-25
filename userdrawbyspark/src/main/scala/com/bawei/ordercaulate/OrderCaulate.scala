package com.bawei.ordercaulate

import java.net.URI
import java.text.DecimalFormat
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
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

  var checkpointDirectory = ""
  var param: Array[String] = null

  @volatile private var instance: Broadcast[Array[(Long, Long, String)]] = null

  def getInstance(sc: SparkContext,rules: Array[(Long, Long, String)]): Broadcast[Array[(Long, Long, String)]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.broadcast(rules)
        }
      }
    }
    instance
  }


  def functionToCreateContext(): StreamingContext = {
    val conf = new SparkConf().setAppName("ordercaulate")
    val ssc = new StreamingContext(conf, Seconds(10))

    ssc.checkpoint(checkpointDirectory)

    val ipText = ssc.sparkContext.textFile("hdfs://node4:8020/ip/rule/ip.txt")
    val ruleRdd: RDD[(Long, Long, String)] = ipText.map(line => {
      val arr = line.split("[|]")
      (arr(2).toLong, arr(3).toLong, arr(6))
    })

    val rules: Array[(Long, Long, String)] = ruleRdd.collect()

    val rulesRef: Broadcast[Array[(Long, Long, String)]] = getInstance(ssc.sparkContext,rules)

    val Array(brokers, groupId, topics) = param
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

        //实时存入hdfs，供离线计算使用

        val df = new DecimalFormat("00")

        val c = Calendar.getInstance()
        val year = c.get(Calendar.YEAR)
        val month = df.format((c.get(Calendar.MONTH) + 1))

        val day = df.format(c.get(Calendar.DAY_OF_MONTH))
        val hour = df.format(c.get(Calendar.HOUR_OF_DAY))
        orders
          .coalesce(1)   //减少输出到hdfs中的文件数量
          .saveAsTextFile("hdfs://node4:8020/ordercaulate/"+year+"/"+month + "/"+day+"/"+hour+"/"+c.getTimeInMillis)

        //计算业务指标
        if(!orders.isEmpty()) {
          //计算实时交易总金额
          CaulateUtil.caulateOrderSum(orders)
          //按地区计算交易额
          CaulateUtil.caulateByProvince(orders,rulesRef)
          //按商品类别统计
          CaulateUtil.caulateByItem(orders)
        }

        //---------------------------

        //提交kafka偏移量
        messages.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    })
    ssc
  }

  def main(args: Array[String]): Unit = {

    checkpointDirectory = "hdfs://node4:8020/sparkcheckpoints"
    param = args
    val context = StreamingContext.getOrCreate(checkpointDirectory,functionToCreateContext _)

    context.start()
    val checkIntervalMillis = 10000
    var isStopped = false


    while (! isStopped) {
      println("calling awaitTerminationOrTimeout")
      isStopped = context.awaitTerminationOrTimeout(checkIntervalMillis)
      if (isStopped)
        println("confirmed! The streaming context is stopped. Exiting application...")
      else
        println("Streaming App is still running. Timeout...")
      checkShutdownMarker
      if (!isStopped && stopFlag) {
        println("stopping ssc right now")
        context.stop(true, true)
        println("ssc is stopped!!!!!!!")
      }
    }
  }

  var stopFlag:Boolean = false
  val shutdownMarker = "hdfs://node4:8020/sparkstreamingstopmark/shutdownmarker"

  def checkShutdownMarker = {
    if (!stopFlag) {
      val fs = FileSystem.get(URI.create("hdfs://node4:8020"),new Configuration())
      stopFlag = fs.exists(new Path(shutdownMarker))
    }

  }
}
