package com.bawei.userdraw.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UserDrawStepOneMapReduce {
    public static class UserDrawStepOneMapper extends Mapper<LongWritable, Text,Text,LongWritable> {

        private Text k;
        private LongWritable v;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            k = new Text();
            v = new LongWritable();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] arr = value.toString().split("[|]");
            try {
                String mdn = arr[0];
                Integer appId = Integer.valueOf(arr[15]);
                Long ProcedureTime = Long.valueOf(arr[12]);
                k.set(mdn+"|"+appId);
                v.set(ProcedureTime);
                context.write(k,v);
            } catch (Exception e) {

            }

        }
    }

    public static class UserDrawStepOneReducer extends Reducer<Text,LongWritable,Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sumTime = 0L;
            String mdnAppId = key.toString();
            for(LongWritable value : values) {
                sumTime = sumTime + value.get();
            }
            context.write(new Text(mdnAppId+"|"+sumTime),NullWritable.get());
        }
    }
}
