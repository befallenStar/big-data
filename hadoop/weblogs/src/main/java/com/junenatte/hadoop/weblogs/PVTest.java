package com.junenatte.hadoop.weblogs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class PVTest {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            FileSystem hdfs = FileSystem.get(conf);
            Path dst = new Path("/main/weblogip_output/");
            if (hdfs.exists(dst))
                hdfs.delete(dst, true);
            Job job = Job.getInstance(conf);

            job.setJarByClass(PVTest.class);

            job.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.setInputPaths(job, "/main/sjh/weblog/*.txt");
            job.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setOutputPath(job, dst);

            job.setMapperClass(PVMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setReducerClass(PVReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            boolean b = job.waitForCompletion(true);
        } catch (InterruptedException | IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
