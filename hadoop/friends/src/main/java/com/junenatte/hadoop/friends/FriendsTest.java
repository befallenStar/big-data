package com.junenatte.hadoop.friends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class FriendsTest {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            FileSystem hdfs = FileSystem.get(conf);
            Path dst = new Path("/main/friends_output/");
            if (hdfs.exists(dst))
                hdfs.delete(dst, true);
            Job job = Job.getInstance(conf);

            job.setJarByClass(FriendsTest.class);

            job.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.setInputPaths(job, "/main/sjh/friends.txt");
            job.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setOutputPath(job, dst);

            job.setMapperClass(FriendsMapper.class);

            job.setReducerClass(FriendsReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            boolean b = job.waitForCompletion(true);
        } catch (InterruptedException | IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
