import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
public class SortTest {
    public static void main(String[] args) {
        Configuration conf=new Configuration();
        try{
            FileSystem hdfs= FileSystem.get(conf);
            Path dst=new Path("/main/order_output/");
            if(hdfs.exists(dst))
                hdfs.delete(dst,true);
            Job job=Job.getInstance(conf);

            job.setJarByClass(SortTest.class);

            job.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.setInputPaths(job,"/main/sjh/order*.txt");
            job.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setOutputPath(job,dst);

            job.setMapperClass(SortMapper.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setSortComparatorClass(SortComparatorDESC.class);

            job.setReducerClass(SortReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(IntWritable.class);
            boolean b=job.waitForCompletion(true);
        } catch (InterruptedException | IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
