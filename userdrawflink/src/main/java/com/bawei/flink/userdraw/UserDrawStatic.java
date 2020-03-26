package com.bawei.flink.userdraw;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

//bin/flink run -m yarn-cluster -c com.bawei.flink.userdraw.UserDrawStatic -yjm 1024 -ytm 1024 -yn 1 -ys 2 /opt/userdrawflink-1.0-SNAPSHOT.jar  hdfs://node4:8020/userdraw/input hdfs://node4:8020/userdraw/userTag
public class UserDrawStatic {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> userText = env.readTextFile(args[0]);//"C:\\yarnData\\userDraw\\input");

        FlatMapOperator<String, Tuple3<String, Integer, Long>> userFlated = userText.flatMap(new FlatMapFunction<String, Tuple3<String, Integer, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple3<String, Integer, Long>> out) throws Exception {
                String[] arr = value.split("[|]");
                try {
                    String mdn = arr[0];
                    Integer appId = Integer.valueOf(arr[15]);
                    Long times = Long.valueOf(arr[12]);
                    if(!StringUtils.isEmpty(mdn)) {
                        out.collect(Tuple3.of(mdn,appId,times));
                    }
                } catch (Exception e) {
                }
            }
        });

        ReduceOperator<Tuple3<String, Integer, Long>> userReduced = userFlated.groupBy(new KeySelector<Tuple3<String, Integer, Long>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> getKey(Tuple3<String, Integer, Long> value) throws Exception {
                return Tuple2.of(value.f0, value.f1);
            }
        }).reduce(new ReduceFunction<Tuple3<String, Integer, Long>>() {
            @Override
            public Tuple3<String, Integer, Long> reduce(Tuple3<String, Integer, Long> value1, Tuple3<String, Integer, Long> value2) throws Exception {
                return Tuple3.of(value1.f0, value1.f1, value1.f2 + value2.f2);
            }
        });

        //读取hbase的第一种方法，自己实现configuration，table，conn
//        DataSource<Tuple8<String, Double, Double, Double, Double, Double, Double, Double>> hbaseInput = env.createInput(new TableInputFormat<Tuple8<String, Double, Double, Double, Double, Double, Double, Double>>() {
//
//            transient Configuration conf = null;
//            transient Connection conn = null;
//            @Override
//            protected Scan getScanner() {
//                return scan;
//            }
//
//            @Override
//            protected String getTableName() {
//                return "ns6:t_draw";
//            }
//
//            @Override
//            protected Tuple8<String, Double, Double, Double, Double, Double, Double, Double> mapResultToTuple(Result r) {
//                String mdn = Bytes.toString(r.getRow());
//                double male = Bytes.toDouble(r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("male")));
//                double female = Bytes.toDouble(r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("female")));
//                double age1 = Bytes.toDouble(r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age1")));
//                double age2 = Bytes.toDouble(r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age2")));
//                double age3 = Bytes.toDouble(r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age3")));
//                double age4 = Bytes.toDouble(r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age4")));
//                double age5 = Bytes.toDouble(r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age5")));
//                return Tuple8.of(mdn,male,female,age1,age2,age3,age4,age5);
//            }
//
//            @Override
//            public void configure(org.apache.flink.configuration.Configuration parameters) {
//                conf = HBaseConfiguration.create();
//                conf.set(HConstants.ZOOKEEPER_QUORUM,"node4:2181");
//                try {
//                    conn = ConnectionFactory.createConnection(conf);
//                    table = (HTable) conn.getTable(TableName.valueOf("ns6:t_draw"));
//                    scan = new Scan();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//
//            @Override
//            public void close() throws IOException {
//                if (table != null) {
//                    table.close();
//                }
//                if (conn != null) {
//                    conn.close();
//                }
//            }
//        });


        DataSource<Tuple8<String, Double, Double, Double, Double, Double, Double, Double>> hbaseinput = env.createInput(new TableInputFormat<Tuple8<String, Double, Double, Double, Double, Double, Double, Double>>() {
            @Override
            protected Scan getScanner() {
                scan = new Scan();
                return scan;
            }

            @Override
            protected String getTableName() {
                return "ns6:t_draw";
            }

            @Override
            protected Tuple8<String, Double, Double, Double, Double, Double, Double, Double> mapResultToTuple(Result r) {
                String mdn = Bytes.toString(r.getRow());
                double male = Bytes.toDouble(r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("male")));
                double female = Bytes.toDouble(r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("female")));
                double age1 = Bytes.toDouble(r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age1")));
                double age2 = Bytes.toDouble(r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age2")));
                double age3 = Bytes.toDouble(r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age3")));
                double age4 = Bytes.toDouble(r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age4")));
                double age5 = Bytes.toDouble(r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age5")));
                return Tuple8.of(mdn, male, female, age1, age2, age3, age4, age5);
            }
        });

        FlatMapOperator<Tuple8<String, Double, Double, Double, Double, Double, Double, Double>, Tuple11<String, Double, Double, Double, Double, Double, Double, Double, Integer, Long, Integer>> hbasemsg = hbaseinput.flatMap(new FlatMapFunction<Tuple8<String, Double, Double, Double, Double, Double, Double, Double>, Tuple11<String, Double, Double, Double, Double, Double, Double, Double, Integer, Long, Integer>>() {
            @Override
            public void flatMap(Tuple8<String, Double, Double, Double, Double, Double, Double, Double> value, Collector<Tuple11<String, Double, Double, Double, Double, Double, Double, Double, Integer, Long, Integer>> out) throws Exception {
                if (!StringUtils.isEmpty(value.f0)) {
                    out.collect(Tuple11.of(value.f0, value.f1, value.f2, value.f3, value.f4, value.f5, value.f6, value.f7, 0, 0L, 0));
                }
            }
        });

        MapOperator<Tuple3<String, Integer, Long>, Tuple11<String, Double, Double, Double, Double, Double, Double, Double, Integer, Long, Integer>> usermsg = userReduced.map(new MapFunction<Tuple3<String, Integer, Long>, Tuple11<String, Double, Double, Double, Double, Double, Double, Double, Integer, Long, Integer>>() {
            @Override
            public Tuple11<String, Double, Double, Double, Double, Double, Double, Double, Integer, Long, Integer> map(Tuple3<String, Integer, Long> value) throws Exception {
                return Tuple11.of(value.f0, 0d, 0d, 0d, 0d, 0d, 0d, 0d, value.f1, value.f2, 1);
            }
        });


        DataSource<String> appTagText = env.readTextFile(args[1]); //"C:\\yarnData\\userDraw\\appData");
        MapOperator<String, Tuple9<Integer, String, Double, Double, Double, Double, Double, Double, Double>> userTag = appTagText.map(new MapFunction<String, Tuple9<Integer, String, Double, Double, Double, Double, Double, Double, Double>>() {
            @Override
            public Tuple9<Integer, String, Double, Double, Double, Double, Double, Double, Double> map(String value) throws Exception {
                String[] arr = value.split("[|]");
                return Tuple9.of(Integer.valueOf(arr[0]), arr[1], Double.valueOf(arr[2]), Double.valueOf(arr[3]), Double.valueOf(arr[4]), Double.valueOf(arr[5]), Double.valueOf(arr[6]), Double.valueOf(arr[7]), Double.valueOf(arr[8]));
            }
        });

        GroupReduceOperator<Tuple11<String, Double, Double, Double, Double, Double, Double, Double, Integer, Long, Integer>, Tuple8<String, Double, Double, Double, Double, Double, Double, Double>> res = usermsg
                .union(hbasemsg)
                .groupBy(0)
                .sortGroup(10, Order.ASCENDING)
                .reduceGroup(new RichGroupReduceFunction<Tuple11<String, Double, Double, Double, Double, Double, Double, Double, Integer, Long, Integer>, Tuple8<String, Double, Double, Double, Double, Double, Double, Double>>() {
                    Map<Integer, Tuple8<String, Double, Double, Double, Double, Double, Double, Double>> userTag;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        List<Tuple9<Integer, String, Double, Double, Double, Double, Double, Double, Double>> broadUserTag = getRuntimeContext().getBroadcastVariable("broadUserTag");
                        userTag = new HashMap<>();
                        for (Tuple9<Integer, String, Double, Double, Double, Double, Double, Double, Double> value : broadUserTag) {
                            userTag.put(value.f0, Tuple8.of(value.f1, value.f2, value.f3, value.f4, value.f5, value.f6, value.f7, value.f8));
                        }
                    }

                    @Override
                    public void reduce(Iterable<Tuple11<String, Double, Double, Double, Double, Double, Double, Double, Integer, Long, Integer>> values, Collector<Tuple8<String, Double, Double, Double, Double, Double, Double, Double>> out) throws Exception {
                        Iterator<Tuple11<String, Double, Double, Double, Double, Double, Double, Double, Integer, Long, Integer>> it = values.iterator();
                        Tuple11<String, Double, Double, Double, Double, Double, Double, Double, Integer, Long, Integer> firstNext = it.next();
                        UserDraw userDraw = new UserDraw();
                        if (firstNext.f10 == 0) {
                            //hbase里有数据
                            userDraw.setMdn(firstNext.f0);
                            userDraw.setMale(firstNext.f1);
                            userDraw.setFemale(firstNext.f2);
                            userDraw.setAge1(firstNext.f3);
                            userDraw.setAge2(firstNext.f4);
                            userDraw.setAge3(firstNext.f5);
                            userDraw.setAge4(firstNext.f6);
                            userDraw.setAge5(firstNext.f7);
                        } else {
                            //说明hbase里没有数据，要重新开始计算
                            userDraw.setMdn(firstNext.f0);
                            userDraw.initSex();
                            userDraw.initAge();

                            protraitSexAndAge(userDraw, firstNext);
                        }

                        while (it.hasNext()) {
                            Tuple11<String, Double, Double, Double, Double, Double, Double, Double, Integer, Long, Integer> next = it.next();
                            protraitSexAndAge(userDraw, next);
                        }

                        out.collect(Tuple8.of(userDraw.getMdn(),userDraw.getMale(),userDraw.getFemale(),userDraw.getAge1(),userDraw.getAge2(),userDraw.getAge3(),userDraw.getAge4(),userDraw.getAge5()));
                    }


                    private void protraitSexAndAge(UserDraw userDraw, Tuple11<String, Double, Double, Double, Double, Double, Double, Double, Integer, Long, Integer> log) {
                        Tuple8<String, Double, Double, Double, Double, Double, Double, Double> tag = userTag.get(log.f8);
                        if (tag != null) {
                            //说明有appid对应的标签
                            userDraw.protraitSex(tag.f1, tag.f2, log.f9);
                            userDraw.protraitAge(tag.f3, tag.f4, tag.f5, tag.f6, tag.f7, log.f9);
                        }
                    }
                }).withBroadcastSet(userTag, "broadUserTag");


        DataSink<Tuple8<String, Double, Double, Double, Double, Double, Double, Double>> output = res.output(new OutputFormat<Tuple8<String, Double, Double, Double, Double, Double, Double, Double>>() {

            org.apache.hadoop.conf.Configuration conf;
            Connection conn;
            Table table;

            @Override
            public void configure(Configuration parameters) {
            }

            @Override
            public void open(int taskNumber, int numTasks) throws IOException {
                conf = HBaseConfiguration.create();
                try {
                    conn = ConnectionFactory.createConnection(conf);
                    table = conn.getTable(TableName.valueOf("ns6:t_draw"));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void writeRecord(Tuple8<String, Double, Double, Double, Double, Double, Double, Double> record) throws IOException {
                Put put = new Put(Bytes.toBytes(record.f0));
                put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("male"), Bytes.toBytes(record.f1));
                put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("female"), Bytes.toBytes(record.f2));
                put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("age1"), Bytes.toBytes(record.f3));
                put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("age2"), Bytes.toBytes(record.f4));
                put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("age3"), Bytes.toBytes(record.f5));
                put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("age4"), Bytes.toBytes(record.f6));
                put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("age5"), Bytes.toBytes(record.f7));

                table.put(put);
            }

            @Override
            public void close() throws IOException {
                if (table != null) {
                    table.close();
                }
                if (conn != null) {
                    conn.close();
                }
            }
        });

        env.execute();

//        List<Tuple8<String, Double, Double, Double, Double, Double, Double, Double>> resC = res.collect();
//        System.out.println(resC.get(1));
//        System.out.println(resC.size());
    }
}
