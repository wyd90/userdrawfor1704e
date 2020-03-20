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
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

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
      var appId = 0
      var times = -1L;
      try {
        mdn = arr(0)
        appId = arr(15).toInt
        times = arr(12).toLong
      } catch {
        case _: Exception =>
      }
      (mdn, ""+appId, times)
    })

    val filtered = maped.filter(x => {
      if ("NULL".equals(x._1) || "NULL".equals(x._2) || (-1L).equals(x._3)) {
        false
      } else {
        true
      }
    })

    val mdnAndAppIdUseTimes: RDD[((String, String), Long)] = filtered.map(x => ((x._1,x._2),x._3)).reduceByKey(_ + _)

    val logs: RDD[(String,(Double, Double, Double, Double, Double, Double, Double, String, Long,String))] = mdnAndAppIdUseTimes.map(x => {
      (x._1._1,(-1d, -1d, -1d, -1d, -1d, -1d, -1d, x._1._2, x._2,"logmsg"))
    })


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
    val fromhbase: RDD[(String,(Double, Double, Double, Double, Double, Double,Double,String,Long,String))] = userDrawData.map(x => {
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
        , Bytes.toDouble(age5Byte)
        ,"NULL"    //appId
        ,-1L
        ,"hbasemsg"))      //times
    })

    val datas: RDD[(String, (Double, Double, Double, Double, Double, Double, Double, String, Long,String))] = logs.union(fromhbase)



    val partitioned: RDD[(String, (Double, Double, Double, Double, Double, Double, Double, String, Long, String))] = datas.partitionBy(new MyPartitioner(4))

    implicit val userOrdering = new Ordering[(String, (Double, Double, Double, Double, Double, Double, Double, String, Long,String))] {
      override def compare(x: (String, (Double, Double, Double, Double, Double, Double, Double, String, Long, String)), y: (String, (Double, Double, Double, Double, Double, Double, Double, String, Long, String))): Int = {
        if(x._1.compareTo(y._1) == 0) {
          if(x._2._10.compareTo(y._2._10) == 0) {
            x._2._8.compareTo(y._2._8)
          } else {
            if("hbasemsg".equals(y._2._10)) {
              1
            } else {
              -1
            }
          }
        } else {
          x._1.compareTo(y._1)
        }
      }
    }


    val sorted: RDD[(String, (Double, Double, Double, Double, Double, Double, Double, String, Long, String))] = partitioned.filter(x => {
      if ("".equals(x._1)) {
        false
      } else {
        true
      }
    }).sortBy(x => x)



    //sorted.take(20).foreach(x => println(x))

    val res: RDD[UserDraw] = sorted.mapPartitions(it => {
      var oldmdn = ""
      //获取用户画像标签库
      val rule = ruleMapBroad.value
      val userDraws = new collection.mutable.ListBuffer[UserDraw]
      var userDraw: UserDraw = null;
      it.foreach(x => {
        if (!x._1.equals(oldmdn)) {
          //新开始的一组了
          oldmdn = x._1
          if (userDraw != null) {
            val toAddUserDraw = new UserDraw(userDraw.mdn, userDraw.male, userDraw.female, userDraw.age1, userDraw.age2, userDraw.age3, userDraw.age4, userDraw.age5)
            userDraws.append(toAddUserDraw)
          }
          userDraw = new UserDraw(x._1)
          if (x._2._10.equals("hbasemsg")) {
            //hbase中有之前的用户画像
            userDraw.male = x._2._1
            userDraw.female = x._2._2
            userDraw.age1 = x._2._3
            userDraw.age2 = x._2._4
            userDraw.age3 = x._2._5
            userDraw.age4 = x._2._6
            userDraw.age5 = x._2._7
          } else {
            //hbase中没有用户画像，开始画像
            val maybeTuple: Option[(String, String, Double, Double, Double, Double, Double, Double, Double)] = rule.get(x._2._8)
            maybeTuple match {
              case Some(a) => {
                userDraw.protraitSex(a._3, a._4, x._2._9)
                userDraw.protraitAge(a._5, a._6, a._7, a._8, a._9, x._2._9)
              }
              case None =>
            }
          }
        } else {
          //是同一组
          rule.get(x._2._8) match {
            case Some(a) => {
              userDraw.protraitSex(a._3, a._4, x._2._9)
              userDraw.protraitAge(a._5, a._6, a._7, a._8, a._9, x._2._9)
            }
            case None =>
          }
        }
      })
      userDraws.toIterator
    })

    res.take(100).foreach(x => println(x))
    //userDrawRes.collect()//.foreach(x => {println(x)})
    //TODO save hbase ...
    sc.stop()

  }

}

class MyPartitioner(partitionNum: Int) extends Partitioner {

  override def numPartitions: Int = partitionNum

  override def getPartition(key: Any): Int = {
    val mdn = key.asInstanceOf[String]
    Math.abs(mdn.hashCode % partitionNum)
  }
}
