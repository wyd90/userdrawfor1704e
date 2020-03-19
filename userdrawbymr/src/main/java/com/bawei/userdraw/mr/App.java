package com.bawei.userdraw.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class App {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName("UserDrawStepOneMapReduce");
        job.setJarByClass(App.class);

        job.setMapperClass(UserDrawStepOneMapReduce.UserDrawStepOneMapper.class);
        job.setReducerClass(UserDrawStepOneMapReduce.UserDrawStepOneReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path("file:///C:\\yarnData\\userDraw\\input"));
        FileOutputFormat.setOutputPath(job, new Path("file:///C:\\yarnData\\userDraw\\output"));

        if(job.waitForCompletion(true)) {
            Job job2 = Job.getInstance(conf);
            job2.setJobName("UserDrawStepTwoMapReduce");
            job2.setJarByClass(App.class);

            job2.setMapperClass(UserDrawStepTwoMapReduce.UserDrawStepTwoMapper.class);
            job2.setReducerClass(UserDrawStepTwoMapReduce.UserDrawStepTwoReducer.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job2, new Path("file:///C:\\yarnData\\userDraw\\output"));
            FileOutputFormat.setOutputPath(job2, new Path("file:///C:\\yarnData\\userDraw\\output2"));

            if(job2.waitForCompletion(true)) {
                conf.set("hbase.zookeeper.quorum","node4:2181");
                Job job3 = Job.getInstance(conf);

                job3.setJobName("UserDrawSaveHBase");
                job3.setJarByClass(App.class);

                //设置表名
                TableMapReduceUtil.initTableReducerJob("ns6:t_draw", UserDrawSaveHBaseMapReduce.UserDrawSaveHBaseReducer.class,job3);
                job3.setMapperClass(UserDrawSaveHBaseMapReduce.UserDrawSaveHBaseMapper.class);

                job3.setMapOutputKeyClass(Text.class);
                job3.setMapOutputValueClass(NullWritable.class);
                job3.setOutputKeyClass(NullWritable.class);
                job3.setOutputValueClass(Put.class);

                FileInputFormat.addInputPath(job3, new Path("file:///C:\\yarnData\\userDraw\\output2"));
                job3.waitForCompletion(true);
            }
        }
    }
}
