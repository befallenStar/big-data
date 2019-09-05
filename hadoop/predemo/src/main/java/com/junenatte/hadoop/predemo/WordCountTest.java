package com.junenatte.hadoop.predemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class WordCountTest {
    public static void main(String[] args) {
        Configuration conf=new Configuration();
        try{
            FileSystem hdfs= FileSystem.get(conf);
            Path dst=new Path("/main/wordcount_output/");
            if(hdfs.exists(dst))
                hdfs.delete(dst,true);
            Job job=Job.getInstance(conf);
            job.setJarByClass(WordCountTest.class);
            job.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.setInputPaths(job,"/main/sjh/*.txt");
            job.setMapperClass(WordCountMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setReducerClass(WordReducer.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            TextOutputFormat.setOutputPath(job,dst);
            boolean b=job.waitForCompletion(true);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
