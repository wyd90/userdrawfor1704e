package com.bawei.userdraw.mr;

import com.bawei.userdraw.bean.StepTwoBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class UserDrawStepTwoPartitioner extends Partitioner<StepTwoBean, NullWritable> {

    //按mdn分区
    public int getPartition(StepTwoBean stepTwoBean, NullWritable nullWritable, int numPartitions) {
        return (stepTwoBean.getMdn().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
