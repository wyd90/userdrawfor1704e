package com.bawei.userdraw.mr;

import com.bawei.userdraw.bean.UserDraw;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class UserDrawStepTwoMapReduce {

    public static class UserDrawStepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text k;
        private Text v;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            k = new Text();
            v = new Text();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] arr = value.toString().split("[|]");
            if(arr.length == 3) {
                String mdn = arr[0];
                String appId = arr[1];
                String procedureTime = arr[2];
                k.set(mdn);
                v.set(appId+"|"+procedureTime);
                context.write(k,v);
            }
        }
    }


    public static class UserDrawStepTwoReducer extends Reducer<Text,Text,Text, NullWritable> {


        private Map<String,String> appTagMap;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(URI.create("file:///"), conf);
            FSDataInputStream in = fs.open(new Path("file:///C:\\yarnData\\userDraw\\appData\\appTab.txt"));
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line = null;
            appTagMap = new HashMap<String, String>();
            while ((line = br.readLine()) != null) {
                String[] arr = line.split("[|]");
                String appId = arr[0];
                String value = arr[0] + "," + arr[1] + "," + arr[2] + "," + arr[3] + "," + arr[4] + "," + arr[5] + "," + arr[6] + "," + arr[7] + "," + arr[8];
                appTagMap.put(arr[0],value);
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String mdn = key.toString();
            if(!StringUtils.isEmpty(mdn)) {
                UserDraw userDraw = new UserDraw();
                //设置用户画像的mdn
                userDraw.setMdn(mdn);
                //初始化性别和年龄
                userDraw.initSex();
                userDraw.initAge();
                for(Text value : values) {
                    String[] arr = value.toString().split("[|]");
                    String appId = arr[0];

                    String appTagMsg = appTagMap.get(appId);
                    if(!StringUtils.isEmpty(appTagMsg)) {
                        String[] split = appTagMsg.split(",");
                        Double male = Double.valueOf(split[2]);
                        Double female = Double.valueOf(split[3]);
                        Long times = Long.valueOf(arr[1]);
                        userDraw.protraitSex(male,female,times);
                        Double age1 = Double.valueOf(split[4]);
                        Double age2 = Double.valueOf(split[5]);
                        Double age3 = Double.valueOf(split[6]);
                        Double age4 = Double.valueOf(split[7]);
                        Double age5 = Double.valueOf(split[8]);
                        userDraw.protraitAge(age1,age2,age3,age4,age5,times);
                    }
                }

                context.write(new Text(userDraw.getMdn() + "|" + userDraw.getMale() + "|" + userDraw.getFemale() + "|" + userDraw.getAge1()+ "|" + userDraw.getAge2()+ "|" + userDraw.getAge3()+ "|" + userDraw.getAge4()+ "|" + userDraw.getAge5()),NullWritable.get());

            }
        }
    }
}
