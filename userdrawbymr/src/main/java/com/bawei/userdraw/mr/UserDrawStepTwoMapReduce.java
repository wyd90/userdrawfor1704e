package com.bawei.userdraw.mr;

import com.bawei.userdraw.bean.StepTwoBean;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class UserDrawStepTwoMapReduce {

    public static class UserDrawStepTwoMapper extends Mapper<LongWritable, Text, StepTwoBean, NullWritable> {

        private StepTwoBean stepTwoBean;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            stepTwoBean = new StepTwoBean();
            FileSplit fs = (FileSplit) context.getInputSplit();
            String path = fs.getPath().toString();
            if(path.contains("fromhbase")) {
                stepTwoBean.setFlag("hbasemsg");
            } else {
                stepTwoBean.setFlag("logmsg");
            }

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] arr = value.toString().split("[|]");

            if("hbasemsg".equals(stepTwoBean.getFlag())) {
                String mdn = arr[0];
                Double male = Double.valueOf(arr[1]);
                Double female = Double.valueOf(arr[2]);
                Double age1 = Double.valueOf(arr[3]);
                Double age2 = Double.valueOf(arr[4]);
                Double age3 = Double.valueOf(arr[5]);
                Double age4 = Double.valueOf(arr[6]);
                Double age5 = Double.valueOf(arr[7]);

                stepTwoBean.set(mdn,male,female,age1,age2,age3,age4,age5,"NULL",-1L);

                context.write(stepTwoBean, NullWritable.get());
            } else {
                if(arr.length == 3) {
                    String mdn = arr[0];
                    String appId = arr[1];
                    Long times = Long.valueOf(arr[2]);
                    stepTwoBean.set(mdn,-1d,-1d,-1d,-1d,-1d,-1d,-1d,appId,times);
                    context.write(stepTwoBean, NullWritable.get());
                }
            }




        }
    }


    public static class UserDrawStepTwoReducer extends Reducer<StepTwoBean,NullWritable,Text, NullWritable> {


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
        protected void reduce(StepTwoBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<NullWritable> it = values.iterator();
            it.next();
            String mdn = key.getMdn();
            if(!StringUtils.isEmpty(mdn)) {
                UserDraw userDraw = new UserDraw();
                //设置用户画像的mdn
                userDraw.setMdn(mdn);

                //hbase中有之前的画像数据
                if("hbasemsg".equals(key.getFlag())) {
                    Double male = key.getMale();
                    Double female = key.getFemale();
                    Double age1 = key.getAge1();
                    Double age2 = key.getAge2();
                    Double age3 = key.getAge3();
                    Double age4 = key.getAge4();
                    Double age5 = key.getAge5();

                    userDraw.setMale(male);
                    userDraw.setFemale(female);
                    userDraw.setAge1(age1);
                    userDraw.setAge2(age2);
                    userDraw.setAge3(age3);
                    userDraw.setAge4(age4);
                    userDraw.setAge5(age5);
                } else {
                    //hbase中没有之前的画像信息
                    userDraw.initSex();
                    userDraw.initAge();

                    String appId = key.getAppId();
                    Long times = key.getTimes();
                    this.protraitUserDraw(appId,times,userDraw);
                }

                while (it.hasNext()) {
                    it.next();
                    Long times = key.getTimes();
                    String appId = key.getAppId();
                    this.protraitUserDraw(appId,times,userDraw);
                }

                context.write(new Text(userDraw.getMdn() + "|" + userDraw.getMale() + "|" + userDraw.getFemale() + "|" + userDraw.getAge1()+ "|" + userDraw.getAge2()+ "|" + userDraw.getAge3()+ "|" + userDraw.getAge4()+ "|" + userDraw.getAge5()),NullWritable.get());

            }
        }


        private void protraitUserDraw(String appId, Long times,UserDraw userDraw) {
            String appTagMsg = appTagMap.get(appId);
            if(!StringUtils.isEmpty(appTagMsg)) {
                String[] split = appTagMsg.split(",");
                Double male = Double.valueOf(split[2]);
                Double female = Double.valueOf(split[3]);
                userDraw.protraitSex(male,female,times);
                Double age1 = Double.valueOf(split[4]);
                Double age2 = Double.valueOf(split[5]);
                Double age3 = Double.valueOf(split[6]);
                Double age4 = Double.valueOf(split[7]);
                Double age5 = Double.valueOf(split[8]);
                userDraw.protraitAge(age1,age2,age3,age4,age5,times);
            }
        }
    }
}
