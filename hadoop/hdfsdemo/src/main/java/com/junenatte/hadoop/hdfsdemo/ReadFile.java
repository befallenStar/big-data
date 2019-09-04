package com.junenatte.hadoop.hdfsdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class ReadFile {
    public static void main(String[] args) {
        Configuration conf=new Configuration();
        try{
            FileSystem hdfs=FileSystem.get(conf);
            Path dst=new Path("/main/test/append.txt");
            FSDataInputStream input=hdfs.open(dst);
            int ci=input.read();
            while(ci!=-1){
                System.out.println((char)ci);
                ci=input.read();
            }
            input.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
