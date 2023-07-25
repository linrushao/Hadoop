package com.gzgs.mr.A8_comparable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {
    private Long upFlow;
    private Long downFlow;
    private Long sumFlow;

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void setSumFlow(){
        this.sumFlow= this.getUpFlow()+getDownFlow();
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);

    }

    @Override
    public void readFields(DataInput in) throws IOException {

        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();

    }

    @Override
    public String toString(){
        return upFlow +"\t"+downFlow+"\t"+sumFlow;
    }


    /**
     * 总流量倒序
     * @param o
     * @return
     */
    @Override
    public int compareTo(FlowBean o) {
        return -this.getSumFlow().compareTo(o.getSumFlow());
    }
}
