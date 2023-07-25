package com.gzgs.mr.A7_mypartitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * 泛型指定为Mapper输出的kv的类型
 */
public class PhonePartitioner extends Partitioner<Text,FlowBean> {

    /**
     * 需求：按照手机号的前三位进行分区操作
     * @param text
     * @param flowBean
     * @param numPartitions
     * @return
     */

    @Override
    public int getPartition(Text text,FlowBean flowBean,int numPartitions){
        //1.获取到的手机号
        String phoneNum = text.toString();
        //2.开始分区
        int partition;
        if(phoneNum.startsWith("126")){
            partition = 0;
        }else if(phoneNum.startsWith("136")){
            partition = 1;
        }else if(phoneNum.startsWith("137")){
            partition = 2;
        }else if(phoneNum.startsWith("138")){
            partition =3;
        }else {
            partition = 4;
        }
        return partition;
    }

}
