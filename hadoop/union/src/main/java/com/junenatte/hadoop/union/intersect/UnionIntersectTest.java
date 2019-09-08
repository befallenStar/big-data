package com.junenatte.hadoop.union.intersect;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class UnionIntersectTest {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            FileSystem hdfs = FileSystem.get(conf);
            Path dst = new Path("/main/unionintersect_output/");
            if (hdfs.exists(dst))
                hdfs.delete(dst, true);
            Job job = Job.getInstance(conf);

            job.setJarByClass(UnionIntersectTest.class);

            job.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.setInputPaths(job, "/main/sjh/union/*.txt");
            job.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setOutputPath(job, dst);

            job.setMapperClass(UnionIntersectMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(UnionIntersectReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            boolean b = job.waitForCompletion(true);
        } catch (InterruptedException | IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}
