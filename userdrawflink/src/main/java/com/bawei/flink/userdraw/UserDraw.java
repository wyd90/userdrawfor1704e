package com.bawei.flink.userdraw;

public class UserDraw {
    private String mdn;
    private double male;
    private double female;
    private double age1;
    private double age2;
    private double age3;
    private double age4;
    private double age5;


    public void initSex() {
        this.male = 0.5;
        this.female = 0.5;
    }

    public void initAge() {
        this.age1 = 0.2;
        this.age2 = 0.2;
        this.age3 = 0.2;
        this.age4 = 0.2;
        this.age5 = 0.2;
    }

    //这个一般算法部门给
    public void protraitSex(Double male, Double female, Long times) {
        Double sum = (this.male + this.female + (male + female) * times);
        if (sum != 0d) {
            this.male = (this.male + male * times) / sum;
            this.female = (this.female + female * times) / sum;
        }

    }

    public void protraitAge(Double age1,Double age2, Double age3, Double age4, Double age5, Long times) {
        Double sum = (this.age1 + this.age2 + this.age3 + this.age4 + this.age5 + (age1 + age2 +age3 + age4 + age5) * times);
        if(sum != 0d) {
            this.age1 = (this.age1 + age1 * times) / sum;
            this.age2 = (this.age2 + age2 * times) / sum;
            this.age3 = (this.age3 + age3 * times) / sum;
            this.age4 = (this.age4 + age4 * times) / sum;
            this.age5 = (this.age5 + age5 * times) / sum;
        }
    }


    public String getMdn() {
        return mdn;
    }

    public void setMdn(String mdn) {
        this.mdn = mdn;
    }

    public double getMale() {
        return male;
    }

    public void setMale(double male) {
        this.male = male;
    }

    public double getFemale() {
        return female;
    }

    public void setFemale(double female) {
        this.female = female;
    }

    public double getAge1() {
        return age1;
    }

    public void setAge1(double age1) {
        this.age1 = age1;
    }

    public double getAge2() {
        return age2;
    }

    public void setAge2(double age2) {
        this.age2 = age2;
    }

    public double getAge3() {
        return age3;
    }

    public void setAge3(double age3) {
        this.age3 = age3;
    }

    public double getAge4() {
        return age4;
    }

    public void setAge4(double age4) {
        this.age4 = age4;
    }

    public double getAge5() {
        return age5;
    }

    public void setAge5(double age5) {
        this.age5 = age5;
    }

    @Override
    public String toString() {
        return mdn+"|"+male+"|"+female+"|"+age1+"|"+age2+"|"+age3+"|"+age4+"|"+age5;
    }
}
