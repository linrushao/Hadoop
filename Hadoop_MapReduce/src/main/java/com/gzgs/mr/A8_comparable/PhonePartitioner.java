package com.gzgs.mr.A8_comparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PhonePartitioner  extends Partitioner<FlowBean ,Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
        String phoneNum = text.toString();
        int partition;
        if(phoneNum.startsWith("136")){
            partition = 0;
        }else if(phoneNum.startsWith("137")){
            partition = 1;
        }else if(phoneNum.startsWith("138")){
            partition = 2;
        } else if (phoneNum.startsWith("139")) {
            partition = 3;
        }else {
            partition = 4;
        }
        return partition;
    }
}
