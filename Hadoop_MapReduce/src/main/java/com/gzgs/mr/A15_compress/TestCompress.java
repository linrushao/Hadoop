package com.gzgs.mr.A15_compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.*;

public class TestCompress {

    /**
     * 使用支持压缩的输出流将数据写入到文件中
     */
    @Test
    public void TestCompress() throws IOException, ClassNotFoundException {

        //待压缩的文件
        String srcFile = "D:\\Atest\\inputWord\\abc.txt";

        //压缩后的文件
        String destFile = "D:\\Atest\\inputWord\\abc.txt";

        //输入流
        FileInputStream in = new FileInputStream(new File(srcFile));

        //输出流
        //使用的压缩的编解码器
        String className = "org.apache.hadoop.io.compress.DefaultCodec";
        Class<?> classCodec = Class.forName(className);
        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(classCodec, conf);
        FileOutputStream out = new FileOutputStream(new File(destFile + codec.getDefaultExtension()));

        CompressionOutputStream codecOut = codec.createOutputStream(out);

        //读写
        IOUtils.copyBytes(in,codecOut,conf);

        //关闭
        IOUtils.closeStream(in);
        IOUtils.closeStream(codecOut);
    }

    /**
     * 使用支持压缩的输入流将数据从文件中读入
     */
    @Test
    public void TestDeCompress() throws IOException {

        //待解压的文件
        String srcFile = "D:\\Atest\\inputWord\\abc.txt.deflate";

        //解压后的文件
        String desrcFile = "D:\\Atest\\inputWord\\abc.txt";

        //输入流
        Configuration conf = new Configuration();

        CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(new Path(srcFile));

        FileInputStream in = new FileInputStream(new File(srcFile));

        CompressionInputStream codecIn = codec.createInputStream(in);
        //输出流
        FileOutputStream out = new FileOutputStream(new File(desrcFile));

        //读写
        IOUtils.copyBytes(codecIn,out,conf);

        //关闭
        IOUtils.closeStream(codecIn);
        IOUtils.closeStream(out);


    }

}
