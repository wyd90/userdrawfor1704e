package com.bawei.userdraw.mr;

import com.bawei.userdraw.bean.StepTwoBean;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class UserDrawStepTwoGroupingComparator extends WritableComparator {

    public UserDrawStepTwoGroupingComparator() {
        super(StepTwoBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        StepTwoBean o1 = (StepTwoBean)a;
        StepTwoBean o2 = (StepTwoBean)b;

        return o1.getMdn().compareTo(o2.getMdn());
    }
}
