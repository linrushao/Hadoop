package com.gzgs.mr.A13_RedueceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;


/**
 * 两个文件
 * order.txt   订单数据
 * pd.txt     商品数据
 */
public class ReduceJoinMapper extends Mapper<LongWritable, Text,Text,OrderPdBean> {

    private String currentFileName;
    private Text outk = new Text();
    private OrderPdBean outv = new OrderPdBean();

    /**
     * 在maptask执行开始执行前调用一次
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取当前的切片对象
        InputSplit inputSplit = context.getInputSplit();
        FileSplit fileSplit = (FileSplit)inputSplit;
        currentFileName = fileSplit.getPath().toString();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        //1.获取一行数据
        String line = value.toString();

        //2.切割数据
        String[] split =line.split("\t");
        if(currentFileName.contains("order")){
            //订单数据
            //1001  01  1
            //3.封装key
            outk.set(split[1]);

            //4.封装value
            outv.setOrderId(split[0]);
            outv.setPid(split[1]);
            outv.setAmount(Integer.parseInt(split[2]));
            outv.setPname("");
            outv.setTitle("order");

        }else {
            //商品数据
            //01  小米
            //3.封装key
            outk.set(split[0]);
            //4.封装value
            outv.setOrderId("");
            outv.setPid(split[0]);
            outv.setAmount(0);
            outv.setPname(split[1]);
            outv.setTitle("pd");
        }

        //写出
        context.write(outk,outv);
    }
}
