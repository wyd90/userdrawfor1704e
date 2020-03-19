package com.bawei.userdraw.mr;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserDrawSaveHBaseMapReduce {
    public static class UserDrawSaveHBaseMapper extends Mapper<LongWritable, Text,Text, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value,NullWritable.get());
        }
    }

    public static class UserDrawSaveHBaseReducer extends TableReducer<Text,NullWritable,NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            for(NullWritable value : values) {
                String[] arr = key.toString().split("[|]");
                String mdn = arr[0];
                Double male = Double.valueOf(arr[1]);
                Double female = Double.valueOf(arr[2]);
                Double age1 = Double.valueOf(arr[3]);
                Double age2 = Double.valueOf(arr[4]);
                Double age3 = Double.valueOf(arr[5]);
                Double age4 = Double.valueOf(arr[6]);
                Double age5 = Double.valueOf(arr[7]);

                Put put = new Put(Bytes.toBytes(mdn));
                put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("male"),Bytes.toBytes(male));
                put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("female"),Bytes.toBytes(female));
                put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("age1"),Bytes.toBytes(age1));
                put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("age2"),Bytes.toBytes(age2));
                put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("age3"),Bytes.toBytes(age3));
                put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("age4"),Bytes.toBytes(age4));
                put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("age5"),Bytes.toBytes(age5));

                context.write(NullWritable.get(),put);
            }
        }
    }
}
