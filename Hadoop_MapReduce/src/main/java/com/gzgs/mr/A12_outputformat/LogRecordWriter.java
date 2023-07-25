package com.gzgs.mr.A12_outputformat;


import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * 自定义的RecordWritable类，继承RecordWriter类
 */
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    //结果文件
    private String linrushaoPath = "D:\\Atest\\linrushao.log";
    private String otherPath="D:\\Atest\\other.log";

    //输出流
    private FSDataOutputStream linrushaoOut;
    private FSDataOutputStream otherOut;

    //文件系统
    private FileSystem fs;
    public LogRecordWriter(TaskAttemptContext context) throws IOException {
        fs = FileSystem.get(context.getConfiguration());
        linrushaoOut= fs.create(new Path(linrushaoPath));
        otherOut = fs.create(new Path(otherPath));
    }

    public LogRecordWriter(){}

    /**
     * 写出数据的方法
     * @param key  待写出的key
     * @param value  待写出的value
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        //判断key是否包含“linrushao”
        String log = key.toString();
        if(log.contains("linrushao")){
            linrushaoOut.writeBytes(log+"\r");
        }else {
            otherOut.writeBytes(log+"\r");
        }
    }

    /**
     * 关闭方法，可以将一些资源的关闭操作写到该方法中
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(linrushaoOut);
        IOUtils.closeStream(otherOut);
    }
}
