package com.gzgs.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * @Author LRS
 * @Date 2022/10/14 20:57
 * Desc
 */
public class hdfsCreate {
    /**
     *在HDFS上创建/mydir/test2.txt文件
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //配置NameNode地址
        URI uri = new URI("hdfs://master:8020");
        //指定用户名,获取FileSystem对象
        FileSystem fs = FileSystem.get(uri, conf, "root");
        //define new file
        Path dfs = new Path("/mydir/test.txt");
        FSDataOutputStream os = fs.create(dfs, true);
        os.writeBytes("hello,hdfs!");

        //关闭流
        os.close();

        //不需要再操作FileSystem了，关闭
        fs.close();
    }
}
