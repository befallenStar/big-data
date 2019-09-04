package com.junenatte.hadoop.hdfsdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class UploadFile {
    public static void main(String[] args) {
        Configuration conf=new Configuration();
        try{
            FileSystem hdfs=FileSystem.get(conf);
            Path src=new Path("C:\\Users\\ThinkPad\\Desktop\\vi命令大全.txt");
            Path dst=new Path("/main/test/append.txt");
            hdfs.copyFromLocalFile(src,dst);
            System.out.println("文件上传成功");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
