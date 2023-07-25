package com.gzgs.mr.A11_group;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//分组比较器

public class OrderGroupComparator extends WritableComparator {
    public OrderGroupComparator(){
        super(OrderBean.class,true);
    }

    /**
     * 分组比较的规则是：只要订单id相同即可
     * @param a
     * @param b
     * @return
     */

    @Override
    public int compare(WritableComparable a, WritableComparable b){
        OrderBean abean = (OrderBean)a;
        OrderBean bbean = (OrderBean)b;
        return abean.getOrderId().compareTo(bbean.getOrderId());
    }

}
