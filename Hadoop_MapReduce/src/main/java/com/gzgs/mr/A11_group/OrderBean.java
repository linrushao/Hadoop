package com.gzgs.mr.A11_group;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private String orderId;
    private Double price;

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public OrderBean(){}

    /**
     * 订单升序，金额降序
     * @param o
     * @return
     */
    @Override
    public int compareTo(OrderBean o) {
        return this.orderId.compareTo(o.getOrderId())!=0?this.orderId.compareTo(o.getOrderId()):o.getPrice().compareTo(this.price);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.orderId = in.readUTF();
        this.price = in.readDouble();
    }

    @Override
    public String toString(){
        return orderId = "\t"+price;
    }
}
