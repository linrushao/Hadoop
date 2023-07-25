package com.gzgs.mr.A5_writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {

    //写出的value
    FlowBean outv = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long totalUpFlow = 0;
        long totalDownFlow = 0;
//        long totalSumFlow = 0;

        //1.迭代values，将相同key的多个value汇总到一起
        for(FlowBean bean : values){
            totalUpFlow += bean.getUpFlow();
            totalDownFlow += bean.getDownFlow();
//            totalSumFlow += bean.getSumFlow();
        }

        //2.封装value
        outv.setUpFlow(totalUpFlow);
        outv.setDownFlow(totalDownFlow);
        outv.setSumFlow();

        //3.写出
        context.write(key,outv);

    }


}
