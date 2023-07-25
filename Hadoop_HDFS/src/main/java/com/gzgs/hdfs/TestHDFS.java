package com.gzgs.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

/**
 * 客户端性质的开发
 * 1、获取客户端对象
 * 2、调用相关方法实现功能
 * 3、关闭
 */
public class TestHDFS {

    private FileSystem fs;
    private Configuration conf;
    private URI uri;
    private String user;

    /**
     * 问题：文件的上传和下载是如何实现的？
     * 文件的上传：基于IO流把数据从本地写到HDFS
     * 文件的下载：基于IO流把数据从HDFS写到本地
     */

    //回收站
    @Test
    public void testTrash() throws IOException {
        conf.set("fs.trash.interval","1");
        conf.set("fs.trash.checkpoint.interval","1");
        //创建回收站对象
        Trash trash = new Trash(fs,conf);
        String s = conf.get("fs.trash.interval");
        System.out.println("s = "+s);
        trash.moveToTrash(new Path("/Test.txt"));

    }

    @Test
    public void testlistAllFileAndDir() throws IOException {
        listAllFileAndDir("/",fs);
    }

    public void listAllFileAndDir(String path , FileSystem fs) throws IOException {
        //先获取指定path下的所有文件和目录
        FileStatus[] listStatus = fs.listStatus(new Path(path));
        //迭代listStatus数组，判断是文件还是目录
        for(FileStatus status : listStatus){
            if(status.isFile()){
                //文件
                System.out.println("File:"+status.getPath().toString());
            }else{
                //目录
                System.out.println("Dir:" + status.getPath().toString());
                //获取当前目录下的所有的文件和目录
                listAllFileAndDir(status.getPath().toString(),fs);
            }
        }
    }

    @Test
    public void testIODownload() throws IOException {
        //待下载的文件
        String secFile = "/testhdfs/testreturn.txt";
        //目标文件
        String destFile = "D:\\Atest\\testreturn.txt";
        //输入流
        FSDataInputStream fis = fs.open(new Path(secFile));
        //输出流
        FileOutputStream fos = new FileOutputStream(new File(destFile));
        //流的对拷
        IOUtils.copyBytes(fis,fos,conf);
        //关闭
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);

    }

    @Test
    public void testIOUpdate() throws IOException {
        //待上传的文件
        String srcFile = "D:\\Atest\\abc.txt";
        //目标文件
        String destFile = "/abc.txt";
        //输入流
        FileInputStream fis = new FileInputStream(new File(srcFile));
        //输出流
        FSDataOutputStream fos = fs.create(new Path(destFile));
        //流的对拷
//        int i ;
        //加上一个缓冲区
//        byte[] buffer = new byte[1024];
//        while((i = fis.read()) != -1){
//            fos.write(buffer,0,i);
//        }

        IOUtils.copyBytes(fis,fos,conf);
        //关闭流
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

    /**
     * 小总结：
     * FileSystem fs : org.apache.hadoop.hdfs.DistributedFileSystem
     */

    /**
     * 参数的优先级
     *
     * xxx-default.xml < xxx.site.xml < IDEA中添加的xxx-site.xml < 在代码中通过Configuration对象设置
     * @throws IOException
     */

    //打印fs
    @Test
    public void testfs(){
        System.out.println(fs.getClass().getName());
    }

    //文件和目录的删除
    @Test
    public void testDelete() throws IOException {
        //删除文件 传true或者false都可以
        boolean delete = fs.delete(new Path("/test.txt"), false);

        //删除目录 如果目录非空，传true或者false都可以，如果目录非空，则需要传入true进行递归删除
        fs.delete(new Path("/test"),false);

        //走回收站的删除方式
        Trash trash = new Trash(fs,conf);
        String s = conf.get("fs.trash.interval");
        System.out.println(s);

        boolean b = trash.moveToTrash(new Path("/smallfile/Test.txt"));
        System.out.println(b);
    }

    /**
     * 文件和文件夹的判断
     */
    @Test
    public void testListStatus() throws IOException {
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for(FileStatus status : listStatus){
            if(status.isFile()){
                System.out.println("File : "+status.getPath());
            }else {
                System.out.println("Dir : "+status.getPath());
            }
        }
    }

    //文件详情查看
    @Test
    public void testListFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"),true);
        //迭代
        while(listFiles.hasNext()){
            //取出下一个
            LocatedFileStatus fileStatus = listFiles.next();
            //获取文件的详情
            System.out.println("文件的路径：" + fileStatus.getPath());
            System.out.println("文件权限：" + fileStatus.getPermission());
            System.out.println("文件主人："  + fileStatus.getOwner());
            System.out.println("文件组： " + fileStatus.getGroup());
            System.out.println("文件大小：" + fileStatus.getLen());
            System.out.println("文件副本数： " + fileStatus.getReplication());
            System.out.println("文件块大小： " + fileStatus.getBlockSize());
            System.out.println("文件的位置：" + Arrays.toString(fileStatus.getBlockLocations()));
            System.out.println("==================================================");
        }
    }

    /**
     * 文件的改名和移动
     */
    @Test
    public void testRename() throws IOException {
        //改名
/*        boolean b = fs.rename(new Path("/testhdfs/TestReturnArray.txt"),
                new Path("/testhdfs/testreturn.txt"));
                */

        //移动
        fs.rename(new Path("/testhdfs/testreturn.txt"),
                new Path("/testreturn.txt"));
    }

    /**
     * 文件的下载
     */
    @Test
    public void testDownload() throws IOException {
        fs.copyToLocalFile(false,new Path("/testhdfs/TestReturnArray.txt"),new Path("D:/Atest"),true);
    }

    /**
     * 文件的上传
     * @throws IOException
     */
    @Test
    public void testUpload() throws IOException {
        fs.copyFromLocalFile(false,true,new Path("D:/BaiduNetdiskDownload/TestReturnArray.txt"),new Path("/testhdfs"));
    }

    /**
     * @Before注解标注的方法会在所有@Test标注的方法执行之前执行
     */
    @Before
    public void init() throws IOException, InterruptedException  {
        uri = URI.create("hdfs://hadoop201:8020");
        //uri = new URI("hdfs://hadoop201:8020);
        conf = new Configuration();

        //设置副本数
        //conf.set("dfs.replication","1");

        conf.set("fs.trash.interval","1");
        conf.set("fs.trash.checkpoint.interval","1");
//        conf.set("fs.defaultFS","hdfs://hadoop201:8020");

        user = "linrushao";
        fs = FileSystem.get(uri,conf,user);
    }

    /**
     * @After注解标注的方法会在所有@Test标注的方法执行结束后执行
     * @throws IOException
     */
    @After
    public void close() throws IOException {
        fs.close();
    }

    @Test
    public void testHDFS() throws IOException, InterruptedException {
        //1、创建文件系统对象
        URI uri = URI.create("hdfs://hadoop201:8020");
        Configuration conf = new Configuration();
        String user = "linrushao";
        FileSystem fs = FileSystem.get(uri,conf,user);
        System.out.println("fs = "+fs);

        //2、创建一个目录
        boolean b = fs.mkdirs(new Path("/testhdfs"));

        //3、关闭
        fs.close();
    }
}

