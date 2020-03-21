package com.bawei.userdraw

import org.apache.commons.lang.StringUtils
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

    //读取用户每天的日志信息
    val text: RDD[String] = sc.textFile("C:\\yarnData\\userDraw\\input")


    //读取标签库信息
    val tagText: RDD[String] = sc.textFile("C:\\yarnData\\userDraw\\appData")
    //将标签库信息转换为元组
    val tagsRdd: RDD[(String, String, Double, Double, Double, Double, Double, Double, Double)] = tagText.map(line => {
      val arr: Array[String] = line.split("[|]")
      (arr(0), arr(1), arr(2).toDouble, arr(3).toDouble, arr(4).toDouble, arr(5).toDouble, arr(6).toDouble, arr(7).toDouble, arr(8).toDouble)
    })
    //将标签库信息收回driver端
    val rules: Array[(String, String, Double, Double, Double, Double, Double, Double, Double)] = tagsRdd.collect()

    //将标签库信息在driver端转换为map结构，以appId做key
    val ruleMap = new mutable.HashMap[String,(String, String, Double, Double, Double, Double, Double, Double, Double)]()
    rules.foreach(x => {
      ruleMap.put(x._1,(x._1,x._2,x._3,x._4,x._5,x._6,x._7,x._8,x._9))
    })
    //将标签库map广播到executor中
    val ruleMapBroad: Broadcast[mutable.HashMap[String, (String, String, Double, Double, Double, Double, Double, Double, Double)]] = sc.broadcast(ruleMap)


    //清洗用户日志信息，提取有用的部分
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
    //过滤垃圾数据
    val filtered = maped.filter(x => {
      if ("NULL".equals(x._1) || "0".equals(x._2) || (-1L).equals(x._3)) {
        false
      } else {
        true
      }
    })
    //以 用户 + appId 分组，计算这个用户这个appId使用时长
    val mdnAndAppIdUseTimes: RDD[((String, String), Long)] = filtered.map(x => ((x._1,x._2),x._3)).reduceByKey(_ + _)

    println(filtered.map(_._1).distinct().filter(x => {
      if(StringUtils.isEmpty(x)) {
        false
      } else {
        true
      }
    }).count())

    //把用户日志信息变成 pair元组
    val logs: RDD[(String,(Double, Double, Double, Double, Double, Double, Double, String, Long,String))] = mdnAndAppIdUseTimes.map(x => {
      //mdn     男概率，女概率，age1概率，age2概率，age3概率，age4概率，age5概率，appId，这款app使用的时长，标志位：表明这条信息是用户日志信息
      (x._1._1,(-1d, -1d, -1d, -1d, -1d, -1d, -1d, x._1._2, x._2,"logmsg"))
    })

    //从hbase中把用户之前的用户画像调出来
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

    println(userDrawData.count())

    //将hbase中读取到的信息做变换
    val fromhbase: RDD[(String,(Double, Double, Double, Double, Double, Double,Double,String,Long,String))] = userDrawData.map(x => {
      //mdn
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
        ,-1L     //times
        ,"hbasemsg"))  //标志位：表示这条信息来自从hbase读取
    })

    //将用户日志信息和从hbase中取出的用户画像做并
    val datas: RDD[(String, (Double, Double, Double, Double, Double, Double, Double, String, Long,String))] = logs.union(fromhbase)

    val partitioned = datas.filter(x => {
      if (!StringUtils.isEmpty(x._1)) {
        true
      } else {
        false
      }
    }).distinct().partitionBy(new MyPartitioner(4))



    implicit val userOrdering = new Ordering[(String, (Double, Double, Double, Double, Double, Double, Double, String, Long,String))] {
      override def compare(x: (String, (Double, Double, Double, Double, Double, Double, Double, String, Long, String)), y: (String, (Double, Double, Double, Double, Double, Double, Double, String, Long, String))): Int = {
        //先把同mdn的数据排到一起
        if(x._1.compareTo(y._1) == 0) {
          //把hbase的信息排第一个
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

    //将分好区的数据进行排序
    val sorted: RDD[(String, (Double, Double, Double, Double, Double, Double, Double, String, Long, String))] = partitioned.sortBy(x => x)



    val res: RDD[UserDraw] = sorted.mapPartitions(it => {
      var oldmdn = ""
      //从广播变量获取用户画像标签库
      val rule = ruleMapBroad.value
      //定义一个返回数据结构
      val userDraws = new collection.mutable.ListBuffer[UserDraw]
      //搞一个用户画像
      var userDraw: UserDraw = null;
      var i = 0;
      //将分区数据一条一条迭代出来
      it.foreach(x => {
        if (!x._1.equals(oldmdn)) {
          i = i + 1
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
      if (userDraw != null) {
        val toAddUserDraw = new UserDraw(userDraw.mdn, userDraw.male, userDraw.female, userDraw.age1, userDraw.age2, userDraw.age3, userDraw.age4, userDraw.age5)
        userDraws.append(toAddUserDraw)
      }
      println("thread="+ Thread.currentThread().getName  + " i="+i)
      userDraws.toIterator
    })

    //res.take(100).foreach(x => println(x))
    println(res.filter(x => {
      if("".equals(x.mdn)) {
        false
      } else {
        true
      }
    }).count())
    //TODO save hbase ...
    sc.stop()

  }

}

//自定义分区器，按照mdn分器
class MyPartitioner(partitionNum: Int) extends Partitioner {

  override def numPartitions: Int = partitionNum

  override def getPartition(key: Any): Int = {
    val mdn = key.asInstanceOf[String]
    (mdn.hashCode & Integer.MAX_VALUE) % partitionNum
  }
}
