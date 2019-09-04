package com.junenatte.hadoop.hdfsdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class CreateFile {
    public static void main(String[] args) {
        Configuration conf=new Configuration();
        try{
            FileSystem hdfs=FileSystem.get(conf);
            Path dst=new Path("/main/test/append.txt");
            byte[] content="a file from java coding".getBytes();
            FSDataOutputStream output=hdfs.create(dst);
            output.write(content,0,content.length);
            output.flush();
            output.close();
            System.out.println("文件是否存在："+hdfs.exists(dst));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
