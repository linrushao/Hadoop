package com.gzgs.hdfs;

//导入hadoop配置的包
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * @Author LRS
 * @Date 2022/10/14 20:59
 * Desc
 */
public class hdfsDelete {
    /**
     *在HDFS上删除/mydir/test.txt文件
     */
    public static void main(String[] args) throws Exception {
        //先创建一个hadoop配置对象，加载hadoop配置文件，如core-default.xml等配置文件
        Configuration conf = new Configuration();
        //配置NameNode地址，用于远程连接hadoop集群
        URI uri = new URI("hdfs://master:8020");
        //指定用户名,获取FileSystem对象
        FileSystem fs = FileSystem.get(uri, conf, "root");
        //自定义一个路径，用于我们需要操作的目录
        Path dfs = new Path("/mydir/test.txt");
        //操作命令，此处调用删除方法
        //第一个参数为要删除文件的路径
        //第二个参数的意思是：如果该路径是一个目录和设置为true，这会成功删除，如果设置为false，则会报错
        boolean deletefile = fs.delete(dfs, true);
        //打印输出是否删除文件成功，若返回的是true则说明删除文件成功，如返回的为false，则说明删除失败
        System.out.println(deletefile);
        //不需要再操作FileSystem了，关闭
        fs.close();
    }

}
