package com.gzgs.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * @Author LRS
 * @Date 2022/10/14 21:36
 * Desc
 */
public class hdfsGet {

    /**
     * 下载hdfs中的/mydir/test.txt文件到本地
     */
    public static void main(String[] args) throws Exception {
        //先创建一个hadoop配置对象，加载hadoop配置文件，如core-default.xml等配置文件
        Configuration conf = new Configuration();
        //配置NameNode地址，用于远程连接hadoop集群
        URI uri = new URI("hdfs://master:8020");
        //指定用户名,获取FileSystem对象
        FileSystem fs = FileSystem.get(uri, conf, "root");

        //需要下载的文件路径
        Path fromPath = new Path("/mydir/test2.txt");
        //需要存放在本地的路径
        Path toPath = new Path("file:///root/test/");
        //调用API的拷贝方法（从HDFS文件系统拷贝文件到本地）
        // 第一个参数是HDFS文件的路径
        // 第二个参数是本地磁盘的路径
        fs.copyToLocalFile(fromPath, toPath);
        System.out.println("下载成功！！");
        //不需要再操作FileSystem了，关闭
        fs.close();
    }

}
