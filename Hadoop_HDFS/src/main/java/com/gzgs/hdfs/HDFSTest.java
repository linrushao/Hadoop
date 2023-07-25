package com.gzgs.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.net.URI;

/**
 * @Author LRS
 * @Date 2022/9/22 20:20
 * Desc
 */
/**
 * 客户端性质的开发
 * 1、获取客户端对象
 * 2、调用相关方法实现功能
 * 3、关闭
 */
public class HDFSTest {
    private FileSystem fs;
    private Configuration conf;
    private URI uri;
    private String user;

    //文件和目录的删除
    @Test
    public void testDelete() throws IOException, InterruptedException {
        //1、创建文件系统对象
        URI uri = URI.create("hdfs://master:8020");
        Configuration conf = new Configuration();
        String user = "root";
        FileSystem fs = FileSystem.get(uri,conf,user);
        //删除文件 传true或者false都可以
        boolean delete = fs.delete(new Path("/user/root/mytest/a.txt"), false);
        //删除目录 如果目录非空，传true或者false都可以，如果目录非空，则需要传入true进行递归删除
        fs.delete(new Path("/user/root/mytest/a.txt"),false);
        //3、关闭
        fs.close();
    }

    /*
    打印所有文件的路径
     */
    @Test
    public void testlistAllFileAndDir() throws IOException, InterruptedException {
        //1、创建文件系统对象
        URI uri = URI.create("hdfs://master:8020");
        Configuration conf = new Configuration();
        String user = "root";
        FileSystem fs = FileSystem.get(uri,conf,user);

        listAllFileAndDir("/user/root/mytest",fs);
        //3、关闭
        fs.close();
    }


    public void listAllFileAndDir(String path , FileSystem fs) throws IOException {
        //先获取指定path下的所有文件和目录
        FileStatus[] listStatus = fs.listStatus(new Path(path));
        //迭代listStatus数组，判断是文件还是目录
        for(FileStatus status : listStatus){
            if(status.isFile()){
                //打印文件绝对路径
                System.out.println("File:"+status.getPath().toString());
            }else{
                //获取目录并打印
                System.out.println("Dir:" + status.getPath().toString());
                //获取当前目录下的所有的文件和目录
                listAllFileAndDir(status.getPath().toString(),fs);
            }
        }
    }

    /**
     * 虚拟机连接
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testHDFS() throws IOException, InterruptedException {
        //1、创建文件系统对象
        URI uri = URI.create("hdfs://master:8020");
        Configuration conf = new Configuration();
        String user = "root";
        FileSystem fs = FileSystem.get(uri,conf,user);
        System.out.println("fs = "+fs);

        //2、创建一个目录
        boolean b = fs.mkdirs(new Path("/user/root/mytest"));
        System.out.println("目录创建状态："+b);

        //3、关闭
        fs.close();
    }
}
