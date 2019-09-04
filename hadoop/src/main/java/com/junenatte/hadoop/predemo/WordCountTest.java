package com.junenatte.hadoop.predemo;

import org.apache.hadoop.conf.Configuration;
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
            Job job=new Job(conf,"wordcount");
            job.setJarByClass(WordCountTest.class);
            job.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.setInputPaths(job,"/main/sjh/*.txt");
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setMapperClass(WordCountMapper.class);
            job.setReducerClass(WordReducer.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setOutputPath(job,new Path("/main/wordcount_output/"));
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
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
