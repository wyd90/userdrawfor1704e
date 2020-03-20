package com.bawei.userdraw

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object UserDrawStatic {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("userdraw")
    val sc = new SparkContext(conf)

    val text: RDD[String] = sc.textFile("C:\\yarnData\\userDraw\\input")
    val tagText: RDD[String] = sc.textFile("C:\\yarnData\\userDraw\\appData")
    val tagsRdd: RDD[(String, String, Double, Double, Double, Double, Double, Double, Double)] = tagText.map(line => {
      val arr: Array[String] = line.split("[|]")
      (arr(0), arr(1), arr(2).toDouble, arr(3).toDouble, arr(4).toDouble, arr(5).toDouble, arr(6).toDouble, arr(7).toDouble, arr(8).toDouble)
    })

    val rules: Array[(String, String, Double, Double, Double, Double, Double, Double, Double)] = tagsRdd.collect()

    val ruleMap = new mutable.HashMap[String,(String, String, Double, Double, Double, Double, Double, Double, Double)]()

    rules.foreach(x => {
      ruleMap.put(x._1,(x._1,x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9))
    })

    val ruleMapBroad: Broadcast[mutable.HashMap[String, (String, String, Double, Double, Double, Double, Double, Double, Double)]] = sc.broadcast(ruleMap)



    val maped: RDD[(String, String, Long)] = text.map(line => {
      val arr: Array[String] = line.split("[|]")
      var mdn = "NULL"
      var appId = "NULL"
      var times = -1L;
      try {
        mdn = arr(0)
        appId = arr(15)
        times = arr(12).toLong
      } catch {
        case _: Exception =>
      }
      (mdn, appId, times)
    })

    val filtered = maped.filter(x => {
      if ("NULL".equals(x._1) || "NULL".equals(x._2) || (-1L).equals(x._3)) {
        false
      } else {
        true
      }
    })

    val mdnAndAppIdUseTimes: RDD[((String, String), Long)] = filtered.map(x => ((x._1,x._2),x._3)).reduceByKey(_ + _)


    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum","node4:2181")
    hbaseConf.set(TableInputFormat.INPUT_TABLE,"ns6:t_draw")

    val scan = new Scan
    val proto: ClientProtos.Scan = ProtobufUtil.toScan(scan)
    val scanToString = Base64.encodeBytes(proto.toByteArray)
    hbaseConf.set(TableInputFormat.SCAN,scanToString)

    val userDrawData: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(hbaseConf
      , classOf[TableInputFormat]
      , classOf[ImmutableBytesWritable]
      , classOf[Result])
    val fromhbase: RDD[(String, (Double, Double, Double, Double, Double, Double,Double))] = userDrawData.map(x => {
      val rowKey = Bytes.toString(x._1.get(), x._1.getOffset, x._1.getLength)
      val maleByte: Array[Byte] = x._2.getValue(Bytes.toBytes("f1"), Bytes.toBytes("male"))
      val femaleByte: Array[Byte] = x._2.getValue(Bytes.toBytes("f1"), Bytes.toBytes("female"))
      val age1Byte: Array[Byte] = x._2.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age1"))
      val age2Byte: Array[Byte] = x._2.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age2"))
      val age3Byte: Array[Byte] = x._2.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age3"))
      val age4Byte: Array[Byte] = x._2.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age4"))
      val age5Byte: Array[Byte] = x._2.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age5"))

      (rowKey
        ,(Bytes.toDouble(maleByte)
        , Bytes.toDouble(femaleByte)
        , Bytes.toDouble(age1Byte)
        , Bytes.toDouble(age2Byte)
        , Bytes.toDouble(age3Byte)
        , Bytes.toDouble(age4Byte)
        , Bytes.toDouble(age5Byte)))
    })

    val groupByMdn: RDD[(String, (String, Long))] = mdnAndAppIdUseTimes.map(x => {
      (x._1._1, (x._1._2, x._2))
    })

    val joined: RDD[(String, ((String, Long), Option[(Double, Double, Double, Double, Double, Double, Double)]))] = groupByMdn.leftOuterJoin(fromhbase)

    joined.map(x => {

      val mdn = x._1
      val logData = (x._2)._1
      val fromhbaseDataOP = (x._2)._2

      val fromHBaseData = fromhbaseDataOP.getOrElse((-1d,-1d,-1d,-1d,-1d,-1d,-1d))
      val userDraw = new UserDraw(mdn)
      if(fromHBaseData._1 ne -1d) {
        //hbase中没有该用户的画像

      } else {
        //hhase中有该用户的画像
      }


      val tag: (String, String, Double, Double, Double, Double, Double, Double, Double) = rule.get(log._1).getOrElse(("NULL","NULL",-1d,-1d,-1d,-1d,-1d,-1d,-1d))
      if(!"NULL".equals(tag._1)) {

      }


    })

    sc.stop()

  }

}
