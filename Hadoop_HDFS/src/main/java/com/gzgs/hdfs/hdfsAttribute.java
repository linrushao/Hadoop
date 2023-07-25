package com.gzgs.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * @Author LRS
 * @Date 2022/10/15 8:17
 * Desc
 */
public class hdfsAttribute {

    /**
     *  读取HDFS上的/mydir/test.txt文件属性信息
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

        //获取文件的状态信息，返回的是一个对象
        FileStatus fileStatus = fs.getFileStatus(dfs);
        //对对象进行遍历，
        System.out.println("文件的具体路径："+fileStatus.getPath());
        System.out.println("文件的创建时间："+fileStatus.getAccessTime());
        System.out.println("文件的块大小："+fileStatus.getBlockSize());
        System.out.println("文件的属于组："+fileStatus.getGroup());
        System.out.println("文件的长度："+fileStatus.getLen());
        System.out.println("文件的副本数："+fileStatus.getReplication());
        System.out.println("文件的修改时间："+fileStatus.getModificationTime());
        System.out.println("文件的权限："+fileStatus.getPermission());

        //不需要再操作FileSystem了，关闭
        fs.close();
    }
}
