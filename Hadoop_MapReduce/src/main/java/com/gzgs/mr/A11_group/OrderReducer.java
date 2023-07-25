package com.gzgs.mr.A11_group;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderReducer extends Reducer<OrderBean, NullWritable,OrderBean ,NullWritable> {
    @Override
    protected void reduce(OrderBean key,Iterable<NullWritable> values,Context context) throws IOException, InterruptedException {
        //只需要将每组数据中的第一条数据打印即可（金额最高）
//        context.write(key,values.iterator().next());

        for(NullWritable value:values){
            context.write(key,NullWritable.get());
        }
    }
}
