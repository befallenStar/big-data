import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class DeduplicationTest {
    public static void main(String[] args) {
        Configuration conf=new Configuration();
        try{
            FileSystem hdfs= FileSystem.get(conf);
            Path dst=new Path("/main/deduplication_output/");
            if(hdfs.exists(dst))
                hdfs.delete(dst,true);
            Job job=Job.getInstance(conf);
            job.setJarByClass(DeduplicationTest.class);
            job.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.setInputPaths(job,"/main/sjh/file*.txt");
            job.setMapperClass(DeduplicationMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setReducerClass(DeduplicationReducer.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            TextOutputFormat.setOutputPath(job,dst);
            boolean b=job.waitForCompletion(true);
        } catch (InterruptedException | IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
