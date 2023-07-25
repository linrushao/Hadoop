package com.gzgs.mr.A8_comparable;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//比较器对象
public class FlowWritableComparator extends WritableComparator {

    public FlowWritableComparator(){
        super(FlowBean.class,true);
    }

    //重写compare方法
    @Override
    public int compare(WritableComparable a,WritableComparable b){
        FlowBean abean = (FlowBean)a;
        FlowBean bbean = (FlowBean)b;
        //比较总流量  倒序，所以用负
        return -abean.getSumFlow().compareTo(bbean.getSumFlow());
    }


}
