package com.junenatte.hadoop.hdfsdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class DeleteFile {
    public static void main(String[] args) {
        Configuration conf=new Configuration();
        try{
            FileSystem hdfs=FileSystem.get(conf);
            Path dst=new Path("/main/test/append.txt");
            hdfs.delete(dst,true);
            System.out.println("文件删除结果："+!hdfs.exists(dst));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
