package com.gzgs.mr.A14_mapjoin;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MapJoinMapper extends Mapper<LongWritable,Text, Text, NullWritable> {

    private Map<String,String> pdMap = new HashMap<>();
    private Text outk = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        //计数器
        context.getCounter("Map Join","setup").increment(1);

       //将小表的数据缓存到内存中
        //获取待缓存的数据
        URI[] cacheFiles = context.getCacheFiles();
        URI file = cacheFiles[0];//pd.txt
        //获取文件，系统对象
        FileSystem fs = FileSystem.get(context.getConfiguration());

        //创建输入流
        FSDataInputStream fsDataInputStream = fs.open(new Path(file));

        //将字节流转换成字符流
        BufferedReader br = new BufferedReader(
                new InputStreamReader(fsDataInputStream, "utf-8")
        );
        String line;
        while((line = br.readLine())!=null){
            //处理每行数据
            //01    格力
            String[] split = line.split("\t");

            pdMap.put(split[0],split[1]);
        }

        //关闭流
        IOUtils.closeStream(br);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //计数器
        context.getCounter("Map Join","map").increment(1);



        //正常处理大表数据

        //1.获取到一行数据
        //1001  01  1

        String line = value.toString();
        //2.切割
        String[] split = line.split("\t");
        //3.join
        split[1] = pdMap.get(split[1]);

        //4.封装key
        outk.set(split[0]+"\t"+split[1]+"\t"+split[2]);
        //5.写出
        context.write(outk,NullWritable.get());
    }

    //通过IO的方式

}
