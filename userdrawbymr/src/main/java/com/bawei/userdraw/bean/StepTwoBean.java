package com.bawei.userdraw.bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StepTwoBean implements WritableComparable<StepTwoBean> {

    private String mdn;
    private Double male;
    private Double female;
    private Double age1;
    private Double age2;
    private Double age3;
    private Double age4;
    private Double age5;

    private String appId;
    private Long times;

    //标志这个bean里装的信息是从hbase里来的还是从第一步mapreduce里来的
    private String flag;

    public int compareTo(StepTwoBean o) {
         if(o.getMdn().compareTo(this.mdn) == 0) {
             return o.getMdn().compareTo(this.mdn);
         } else {
             if(o.getFlag().compareTo(this.flag) == 0) {
                 return o.age1.compareTo(this.age1);
             } else {
                 if("hhasemsg".equals(o.getFlag())) {
                     return 1;
                 } else {
                     return -1;
                 }
             }
         }
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(mdn);
        out.writeDouble(male);
        out.writeDouble(female);
        out.writeDouble(age1);
        out.writeDouble(age2);
        out.writeDouble(age3);
        out.writeDouble(age4);
        out.writeDouble(age5);
        out.writeUTF(appId);
        out.writeLong(times);
        out.writeUTF(flag);

    }

    public void readFields(DataInput in) throws IOException {
        this.mdn = in.readUTF();
        this.male = in.readDouble();
        this.female = in.readDouble();
        this.age1 = in.readDouble();
        this.age2 = in.readDouble();
        this.age3 = in.readDouble();
        this.age4 = in.readDouble();
        this.age5 = in.readDouble();
        this.appId = in.readUTF();
        this.times = in.readLong();
        this.flag = in.readUTF();
    }

    public String getMdn() {
        return mdn;
    }

    public void setMdn(String mdn) {
        this.mdn = mdn;
    }

    public Double getMale() {
        return male;
    }

    public void setMale(Double male) {
        this.male = male;
    }

    public Double getFemale() {
        return female;
    }

    public void setFemale(Double female) {
        this.female = female;
    }

    public Double getAge1() {
        return age1;
    }

    public void setAge1(Double age1) {
        this.age1 = age1;
    }

    public Double getAge2() {
        return age2;
    }

    public void setAge2(Double age2) {
        this.age2 = age2;
    }

    public Double getAge3() {
        return age3;
    }

    public void setAge3(Double age3) {
        this.age3 = age3;
    }

    public Double getAge4() {
        return age4;
    }

    public void setAge4(Double age4) {
        this.age4 = age4;
    }

    public Double getAge5() {
        return age5;
    }

    public void setAge5(Double age5) {
        this.age5 = age5;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public Long getTimes() {
        return times;
    }

    public void setTimes(Long times) {
        this.times = times;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public void set(String mdn, Double male, Double female, Double age1, Double age2, Double age3, Double age4, Double age5, String appId, Long times) {
        this.mdn = mdn;
        this.male = male;
        this.female = female;
        this.age1 = age1;
        this.age2 = age2;
        this.age3 = age3;
        this.age4 = age4;
        this.age5 = age5;
        this.appId = appId;
        this.times = times;
    }
}
