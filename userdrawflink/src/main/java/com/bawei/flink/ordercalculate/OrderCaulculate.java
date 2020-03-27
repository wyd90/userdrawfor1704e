package com.bawei.flink.ordercalculate;

import com.bawei.flink.redisutil.IpUtil;
import com.bawei.flink.redisutil.JedisConnectionPool;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import redis.clients.jedis.Jedis;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class OrderCaulculate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node4:9092");
        properties.setProperty("group.id", "flinkordercomsumer");

        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>("flinkorder", new SimpleStringSchema(), properties);
        //consumer.setCommitOffsetsOnCheckpoints(true);
        consumer.setStartFromGroupOffsets();

        DataStreamSource<String> message = env.addSource(consumer);

        //etl
        SingleOutputStreamOperator<Tuple7<String, Long, String, String, Double, Float, Double>> flated = message.flatMap(new FlatMapFunction<String, Tuple7<String, Long, String, String, Double, Float, Double>>() {
            @Override
            public void flatMap(String value, Collector<Tuple7<String, Long, String, String, Double, Float, Double>> out) throws Exception {
                try {
                    String[] arr = value.split(" ");
                    if(arr.length == 6) {
                        Double price = Double.valueOf(arr[4]);
                        Float amount = Float.valueOf(arr[5]);
                        long ipLong = IpUtil.ip2Long(arr[1]);
                        out.collect(Tuple7.of(arr[0],ipLong,arr[2],arr[3],price,amount,price * amount));
                    }
                } catch (Exception e) {

                }
            }
        });

        //计算总交易额
        SingleOutputStreamOperator<String> ordersum = flated.map(new RichMapFunction<Tuple7<String, Long, String, String, Double, Float, Double>, String>() {
            Jedis conn;
            ArrayList<Tuple3<Long,Long,String>> rules;
            @Override
            public void open(Configuration parameters) throws Exception {
                conn = JedisConnectionPool.getConnection();
                //把ip表读出来
                org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
                FileSystem fs = FileSystem.get(URI.create("file:///"), conf);
                FSDataInputStream in = fs.open(new Path("C:\\yarnData\\ip\\rule\\ip.txt"));
                BufferedReader br = new BufferedReader(new InputStreamReader(in));
                String line = null;
                rules = new ArrayList<>();
                while ((line = br.readLine()) != null) {
                    String[] arr = line.split("[|]");
                    rules.add(Tuple3.of(Long.valueOf(arr[2]),Long.valueOf(arr[3]),arr[6]));
                }
            }
            @Override
            public String map(Tuple7<String, Long, String, String, Double, Float, Double> value) throws Exception {
                //计算总交易额
                conn.incrByFloat("ordersum", value.f6);
                //计算品类交易额
                conn.incrByFloat(value.f2,value.f6);
                //按地区计算交易额
                String province = IpUtil.searchIp(rules, value.f1);
                conn.incrByFloat(province,value.f6);

                return value.f0;
            }
            @Override
            public void close() throws Exception {
                conn.close();
            }
        });

        //ordersum.print();

        env.execute();

    }
}
