package com.gzgs.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

/**
 * @Author LRS
 * @Date 2022/10/15 8:17
 * Desc
 */
public class hdfsRead {

    /**
     *  读取HDFS上的/mydir/test.txt文件
     */
    public static void main(String[] args) throws Exception {
        //先创建一个hadoop配置对象，加载hadoop配置文件，如core-default.xml等配置文件
        Configuration conf = new Configuration();
        //配置NameNode地址，用于远程连接hadoop集群
        URI uri = new URI("hdfs://master:8020");
        //指定用户名,获取FileSystem对象
        FileSystem fs = FileSystem.get(uri, conf, "root");
        //自定义一个路径，用于我们需要操作的目录
        Path dfs = new Path("/mydir/test2.txt");
        //调用打开文件的方法,返回的是输入流
        FSDataInputStream files = fs.open(dfs);
        //打印输出文件里的内容
//        System.out.println(files);
        //从文件读取数据 创建一个BufferedReader字符流对象，将上面的字节流转换为字符流
        BufferedReader br = new BufferedReader(new InputStreamReader(files,"utf-8"));
        String line="";
        //循环读取文件里的内容
        while((line=br.readLine())!=null){
            System.out.println(line);
        }
        //关闭字符流
        br.close();
        //不需要再操作FileSystem了，关闭
        fs.close();
    }


}
