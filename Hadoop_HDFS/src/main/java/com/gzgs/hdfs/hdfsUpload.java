package com.gzgs.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * @Author LRS
 * @Date 2022/10/14 21:20
 * Desc
 */
public class hdfsUpload {
    /**
     *  上传本地文件/root/test/txt1.txt到hdfs的/mydir/目录下
     */
    //因为该程序都会涉及到文件的打开关闭，即IO流，所以会有IO异常，直接抛出即可，此处我们抛出最大的异常
    public static void main(String[] args) throws Exception {
        //先创建一个hadoop配置对象，加载hadoop配置文件，如core-default.xml等配置文件
        Configuration conf = new Configuration();
        //配置NameNode地址，用于远程连接hadoop集群
        URI uri = new URI("hdfs://master:8020");
        //指定用户名,获取FileSystem对象
        FileSystem fs = FileSystem.get(uri, conf, "root");
        //自定义本地路径
        Path fromPath=new Path("file:///root/test/txt1.txt");
        //需要上传到hdfs的路径
        Path toPath=new Path("/mydir");
        //调用API的方法拷贝方法
        // 第一个参数是上传文件的本地路径第二个参数是需要保存到hdfs的路径
        fs.copyFromLocalFile(fromPath,toPath);
        System.out.println("上传成功！！");
        //不需要再操作FileSystem了，关闭
        fs.close();
    }

}
